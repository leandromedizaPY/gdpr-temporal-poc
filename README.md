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

## Architecture

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

## Current Limitations (PoC)

Activities are **mocked** with sleeps. To connect to real infrastructure:

| Activity | Client to swap in |
|---|---|
| `AnonymizeBigQueryBatch` | `cloud.google.com/go/bigquery` — single DML `UPDATE ... WHERE user_id IN (...)` |
| `AnonymizeDynamoDBBatch` | `github.com/aws/aws-sdk-go-v2/service/dynamodb` — `BatchWriteItem` |
| `SendCompletionEvent` | `github.com/aws/aws-sdk-go-v2/service/sqs` — `SendMessage` |
| `ExportHistoryToS3` | `github.com/aws/aws-sdk-go-v2/service/s3` — `PutObject` |
| `InMemoryQueue` | Replace with SQS-backed implementation of the `Queue` interface |

See [PLAN.md](PLAN.md) for the full design rationale.
