# GDPR Anonymization Workflow — Temporal PoC

A proof of concept for orchestrating GDPR anonymization requests using [Temporal](https://temporal.io), built in Go.

## The Problem

When a GDPR deletion/anonymization request arrives, user data must be removed from **multiple systems** (BigQuery, DynamoDB, and potentially others). The current implementation using AWS Step Functions has two structural weaknesses:

- **No crash recovery** — if the process dies mid-execution, the partially-anonymized state is left with no automatic recovery.
- **High cost at scale** — running one BQ query per user is expensive. Under high event volume, Step Functions also becomes a bottleneck.

## The Solution

Temporal's durable execution model solves both problems natively:

- **Crash recovery is automatic** — if the worker dies mid-execution, Temporal resumes exactly where it left off on restart. No manual intervention.
- **Batch processing** — requests accumulate in a long-running collector workflow and are drained on a fixed schedule into a single BQ DML statement covering N users, instead of N individual queries.
- **Audit trail built-in** — the Temporal event history is an immutable, timestamped log of every step.

## Today's Production Flow (v2)

```
BnD v2 (central platform)
  │  publish
  ▼
SNS  bnd-v2-{model}-deletion-{env}
  │
  ▼
SQS  bnd-v2-{model}-deletion-queue-{env}  (+ DLQ, max_receive_count=3)
  │  manual ReceiveMessage loop (up to 15,000 events/run, 60 parallel callers)
  ▼
Step Function  step-functions-gdpr-service-{env}  (v2 bolted onto the v1 SF)
  │
  bnd_v2_consumer_lambda  (one shared Lambda across all models)
  │  insert
  BigQuery staging table  gdpr_service_v2_requests_{model}_{env}
  │
  │
  ├─ 1. GDPR update Fulfilled Already lambda invoke
  ├─    Wait 60s / Choice — poll until FA BQ job done             ┐ repeated
  ├─ 2. GDPR update Fulfilled lambda invoke (hashing job)         │ Wait+Choice
  ├─    Wait 60s / Choice — poll until hashing BQ job done        │ polling
  ├─ 3. Update Ban and Deletion lambda invoke                     │ loops
  │      → marks rows FULFILLED, sends ack ONE MESSAGE AT A TIME  │
  │        to the shared response queue                           ┘
  │        sqs-client-ingestion-ban-and-deletion-v2-{env}
  ├─ 4. Wait 5 min → EventBridge put event
  │      → re-triggers this same Step Function for the next cycle
  │
  └─ on any Lambda error: Catch → Wait 15 min →
        EventBridge put event after failure → Fail state
        (still self-re-arms the next cycle — the rows that were
         mid-flight when the failure happened are not auto-recovered)
```

## What Changes With Temporal

Honest comparison — this table only marks something ✅ if it's actually true
of the code in this repo today, not just true of Temporal in general:

| Issue | Today | With Temporal | Status in this PoC |
|---|---|---|---|
| I4 | Mid-pipeline SF failure orphans rows, no auto-recovery | Workflow resumes automatically from the last completed step | ✅ Solved by design |
| I6/I7 | Manual polling loop, hard-coded event cap, Lambda can die mid-drain | Long-running workflows have no execution time limit | ✅ Solved by design |
| I9 | No per-request audit trail; manual CloudWatch log joins by `executionId` | Event history is a built-in per-workflow audit trail | ✅ Built (`ExportHistoryToS3`) |
| *(A3 bug class)* | Async job IDs tracked via shared execution state can desync — a real instance of this was found & fixed in prod | Sequential workflow code — can't mark a row done before the dependent activity call actually returns | ✅ Solved by design |
| *(A2 decision)* | DLQ isn't a retry mechanism — BnD must notice an `error` ack and manually resend | Automatic per-activity `RetryPolicy` | ✅ Solved by design |
| I5 | Replay re-inserts already-`FULFILLED` rows, duplicate ack | Collector dedupes by `request_id` while pending | ⚠️ Partial — reprocessing *after* completion is only assumed idempotent, not guaranteed like I5 intends |
| I2/I3 | Non-customer/invalid messages silently dropped, no ack → infinite resend | — | ❌ Not modeled — no validation/`INVALID` path yet |
| I12 | Acks sent one at a time, not batched | — | ❌ Not fixed — `SendCompletionEvent` is still per-request |
| I8, I10 | 10%-tolerance error model; one bad row fails the whole batch | — | ❌ N/A — BQ/DynamoDB activities are still mocked |
| *(per-model onboarding)* | 5 models, each needing Terraform + ~6 service-repo touch points | — | ❌ Not modeled — this PoC handles one implicit model |

See [PLAN.md](PLAN.md) for the full design rationale. This table needs
updating as the ❌ gaps close.

## PoC Architecture

```
BnD v2
  │  (GDPR deletion event)
  ▼
HTTP :8081/events  (ingest server)
  │  strips PII — only user_id/request_id/source forwarded
  ▼
InMemoryQueue  (swap point for real SQS)
  │
  ▼
Listener
  │  SignalWithStartWorkflow("addRequest", GDPRRequest)
  ▼
GDPRBatchCollectorWorkflow  (long-running, singleton)
  │  accumulates {user_id, request_id, source} via signals
  │  dedupes duplicate request_ids while pending
  │
  │  ◀── Temporal Schedule fires every 1 min ──▶
  │
  ▼
GDPRSchedulerWorkflow
  │  1. query collector → pending snapshot
  │  2. if empty → exit
  │  3. run GDPRProcessorWorkflow as a child:
  │       ├── AnonymizeBigQueryBatch([]userID)   ─┐ parallel
  │       ├── AnonymizeDynamoDBBatch([]userID)   ─┘ one DML per batch
  │       ├── SendCompletionEvent per request
  │       └── ExportHistoryToS3 (this run's own history)
  │  4. on success, signal collector "clearProcessed" with
  │     EXACTLY the request_ids from step 1 — never a blanket
  │     clear, so requests arriving mid-run are never dropped
  ▼
GDPRBatchCollectorWorkflow removes those request_ids from pending state
```

## Production Architecture

The PoC's workflow/activity/signal shape doesn't change for production — only
the boundary components (queue, activity clients, encryption, retention) get
swapped for real infrastructure:

```
┌───────────────┐   send    ┌────────────────────────┐  long-poll  ┌──────────────────────┐
│   BnD v2      │──────────▶│  SQS Input Queue       │────────────▶│  SQS Listener        │
│  (real        │           │  gdpr-requests-prod    │             │  (aws-sdk-go-v2)     │
│   publisher)  │           └────────────────────────┘             └──────────┬───────────┘
└───────────────┘                                                             │ SignalWithStartWorkflow
                                                                              │ ("addRequest", GDPRRequest)
                                                                              ▼
                                                ┌─────────────────────────────────────────────────┐
                                                │  GDPRBatchCollectorWorkflow                     │
                                                │  Temporal Cloud (or self-hosted prod cluster)   │
                                                │  namespace retention: 7 days                    │
                                                │  payloads encrypted (AES-GCM, KMS-backed key)   │
                                                └───────────────────────┬─────────────────────────┘
                                                                        │
                                          Temporal Schedule (every 5 min, prod cadence) ──▶
                                                                        │
                                                                        ▼
                                                ┌────────────────────────────────────────────────┐
                                                │  GDPRSchedulerWorkflow                         │
                                                │  query pending → run child → clearProcessed    │
                                                └───────────────────────┬────────────────────────┘
                                                                        │ child workflow
                                                                        ▼
                                                ┌──────────────────────────────────────────────────┐
                                                │  GDPRProcessorWorkflow                           │
                                                │   ├─ BigQuery: UPDATE ... WHERE user_id IN (...) │
                                                │   ├─ DynamoDB: BatchWriteItem                    │
                                                │   ├─ SendMessageBatch → SQS Output Queue         │
                                                │   └─ S3 PutObject (SSE-KMS, 7-day lifecycle)     │
                                                └───────────────┬──────────────────┬───────────────┘
                                                                │                  │
                                                                ▼                  ▼
                                                ┌───────────────────────┐  ┌────────────────────────────┐
                                                │  SQS Output Queue     │  │  S3 Audit Bucket           │
                                                │  gdpr-responses-prod  │  │  gdpr-workflow-history     │
                                                └───────────────────────┘  │  (7-day lifecycle policy)  │
                                                                           └────────────────────────────┘

Worker fleet: N replicas of the worker process behind the same Temporal task
queue, for horizontal scale.
```

### What's stubbed today vs. not yet built

| Production component | Current PoC stand-in | File | Status |
|---|---|---|---|
| Real AWS SQS input queue | `InMemoryQueue` (in-process channel) | `queue.go` | Stubbed |
| SQS listener (`aws-sdk-go-v2`, long-poll) | `Listener` draining the in-memory queue | `listener.go` | Stubbed |
| BnD v2 publisher | Synthetic event generator | `producer/main.go` | Stubbed |
| `AnonymizeBigQueryBatch` → real BigQuery, `UPDATE ... WHERE user_id IN (...)` | Mocked sleep + heartbeat | `activities.go` | Stubbed |
| `AnonymizeDynamoDBBatch` → real `BatchWriteItem` | Mocked sleep | `activities.go` | Stubbed |
| `SendCompletionEvent(s)` → real SQS `SendMessage`/`SendMessageBatch` | Mocked sleep + log; still per-request, not yet batched (see PLAN.md) | `activities.go` | Stubbed |
| `ExportHistoryToS3` → real S3 `PutObject`, SSE-KMS + 7-day lifecycle | Writes to a local `history/` directory | `activities.go` | Stubbed |
| Payload encryption (Temporal `PayloadCodec`, AES-GCM, KMS-backed key) | **Not implemented** — no codec configured; PII itself is already excluded from every payload (see PII Protection), this would add defense-in-depth for whatever *does* cross Temporal | — | Not yet built |
| Namespace Workflow Execution Retention = 7 days | **Not implemented** — local dev server uses its default retention | — | Not yet built |
| Worker fleet (N replicas behind the task queue) | Single local `go run ./worker` process | `worker/main.go` | Stubbed |
| Temporal Cloud | Local `temporal server start-dev` | — | Stubbed |
| Production schedule interval (5 min) | 1 min demo cadence | `schedule.go` (`ProcessorInterval`) | Stubbed (config only) |


## Temporal Features Used

| Feature | Where |
|---|---|
| **Signals** | Each incoming event is delivered to the batch collector via `SignalWithStartWorkflow`; the scheduler clears processed requests via `SignalExternalWorkflow` |
| **Long-running workflow** | `GDPRBatchCollectorWorkflow` stays alive indefinitely, accumulating requests |
| **Temporal Schedules** | Drive `GDPRSchedulerWorkflow` on a fixed cadence (1 min demo / 5 min production) instead of an internal timer |
| **Child workflows** | `GDPRSchedulerWorkflow` runs `GDPRProcessorWorkflow` as a child per tick |
| **ContinueAsNew** | Resets the collector's event history after 500 cycles to keep it bounded |
| **Parallel activities** | BQ and DynamoDB batch operations run simultaneously |
| **Retry policy** | Each activity retries automatically on failure |
| **Heartbeat** | BigQuery activity heartbeats during long batch jobs |
| **Query handler** | Query the collector's live pending state without interrupting execution |
| **Durable execution** | Worker crash mid-flow → resumes from last checkpoint |

## PII Protection

The full deletion event (email, username, etc.) only lives in memory inside the ingest server. Only `user_id`/`request_id`/`source` are forwarded to Temporal via signal — **PII fields never reach the event history**, and therefore never reach the `ExportHistoryToS3` audit export either. `TestGDPRRequest_NoPIIFields` in `workflows_test.go` guards against a future change accidentally reintroducing a PII field.

For stricter compliance, the signal payload can be reduced to just `request_id` (an opaque UUID), keeping the `user_id` mapping entirely outside of Temporal.

## Project Structure

```
.
├── shared.go          # Types: GDPRRequest, GDPRResult, AnonymizationStatus
├── workflows.go       # GDPRBatchCollectorWorkflow, GDPRSchedulerWorkflow, GDPRProcessorWorkflow + GDPRWorkflow (per-event, reference)
├── activities.go      # AnonymizeBigQueryBatch, AnonymizeDynamoDBBatch, SendCompletionEvent, QueryCollectorPending, ExportHistoryToS3
├── schedule.go        # EnsureSchedulerSchedule — idempotent Temporal Schedule setup
├── queue.go           # Queue interface + InMemoryQueue (swap point for SQS)
├── ingest.go          # HTTP server on :8081 — receives events and enqueues them
├── listener.go        # Drains queue, signals GDPRBatchCollectorWorkflow per message
├── worker/main.go     # Worker process — registers workflows, activities, ingest server, listener, schedule
├── producer/main.go   # Stub producer simulating BnD v2 events
├── starter/main.go    # Direct workflow starter (reference, bypasses queue)
└── compose.yaml       # Docker Compose: Temporal server + worker
```

## Running Locally

**Prerequisites:** Docker and Docker Compose.

```bash
# Start Temporal server + worker (exposes UI on :8080, gRPC on :7233)
docker compose up temporal worker
```

In a separate terminal, send events via the producer:

```bash
# Send 5 synthetic GDPR requests
go run ./producer -count=5
```

The Temporal UI is at [http://localhost:8080](http://localhost:8080). You'll see the `gdpr-batch-collector` workflow accumulate signals, and — on the next Schedule tick (every minute) — a `GDPRSchedulerWorkflow` run that starts a `GDPRProcessorWorkflow` child, processes the batch, and clears it from the collector.

**Without Docker:**

```bash
# Terminal 1 — Temporal dev server
temporal server start-dev

# Terminal 2 — worker + ingest server
TEMPORAL_ADDRESS=localhost:7233 go run ./worker

# Terminal 3 — send events
go run ./producer -count=5
```

## Querying Live Collector State

```bash
temporal workflow query \
  --workflow-id gdpr-batch-collector \
  --type pendingRequests
```

## Triggering a Batch Immediately (Demo)

Rather than waiting for the schedule's 1-minute cadence:

```bash
temporal schedule trigger --schedule-id gdpr-scheduler-schedule
```

See [PLAN.md](PLAN.md) for the full design rationale.
