# Implementation Plan

## Goal

Replace the current AWS Step Functions-based GDPR anonymization pipeline with a Temporal workflow that is:

- **Crash-safe** — resumes automatically from the last checkpoint if a worker dies
- **Cost-efficient** — batches multiple requests into a single BQ DML instead of one query per user
- **PII-safe** — email, username, and other PII fields never reach Temporal's event history
- **Idempotent** — duplicate events from SQS cannot trigger double-processing

---

## What's Built

### Queue Infrastructure

| Component | File | Description |
|---|---|---|
| Queue interface | `queue.go` | Swap point for real SQS — `InMemoryQueue` used locally |
| Ingest server | `ingest.go` | HTTP POST `:8081/events` — receives raw events, strips PII, enqueues |
| Listener | `listener.go` | Drains queue, calls `SignalWithStartWorkflow` per message |
| Producer | `producer/main.go` | Stub simulating BnD v2 publishing deletion events |

### Batch Collector Workflow (`GDPRBatchCollectorWorkflow`)

A long-running, singleton Temporal workflow (Workflow ID `gdpr-batch-collector`) that accumulates incoming `{user_id, request_id, source}` requests and holds them until told to clear them — it never calls BigQuery/DynamoDB itself.

- **`addRequest` signal** (payload: `GDPRRequest`) — adds a request to the pending set, keyed by `request_id`. A duplicate `request_id` already pending is silently ignored (idempotency without a per-user Workflow ID).
- **`clearProcessed` signal** (payload: `[]string` of `request_id`s) — removes exactly the named request_ids, never a blanket clear. New `addRequest` signals landing between a scheduler tick's query and its clear signal are never dropped, since only ids explicitly named are removed.
- **`pendingRequests` query** — returns the current pending set (`[]GDPRRequest`) for inspection or for the scheduler to drain.
- **`ContinueAsNew`** after 500 signal-processing cycles, carrying the current pending set forward, to keep the (indefinitely-running) history bounded.

### Scheduled Processor (`GDPRSchedulerWorkflow` + `GDPRProcessorWorkflow`)

Replaces an internal flush timer with a **Temporal Schedule** (`gdpr-scheduler-schedule`, every `ProcessorInterval` — 1 minute for the demo; see `schedule.go`) that triggers `GDPRSchedulerWorkflow` on a fixed cadence:

```
Temporal Schedule (every 1 min)
  │
  ▼
GDPRSchedulerWorkflow
  │  1. QueryCollectorPending activity → []GDPRRequest snapshot
  │  2. If empty → exit early
  │  3. Execute GDPRProcessorWorkflow as a CHILD workflow, passing the snapshot
  │       ├── AnonymizeBigQueryBatch([]userID)   ─┐ parallel
  │       ├── AnonymizeDynamoDBBatch([]userID)   ─┘
  │       ├── SendCompletionEvent per request
  │       └── ExportHistoryToS3 (this run's own history)
  │  4. On child success: SignalExternalWorkflow(collector, "clearProcessed", []requestID)
  ▼           using EXACTLY the request_ids from step 1's snapshot
GDPRBatchCollectorWorkflow removes those request_ids from its pending state
```

**Key design decision — clear by `request_id`, never a blanket clear:** new requests can arrive on the collector while the scheduler/processor are running. Using the `request_id`s captured in the step-1 snapshot (rather than "clear everything") ensures requests that arrive mid-run are never accidentally dropped. Verified in `workflows_test.go`.

**On processor failure:** the scheduler does **not** signal `clearProcessed` — the batch stays pending and is retried on the next scheduled tick, instead of silently losing it.

**Changes from the previous batch-only design:**
- `GDPRBatchWorkflow` renamed to `GDPRBatchCollectorWorkflow`; the internal flush timer/size threshold is gone — draining is now entirely schedule-driven.
- `addRequest`'s payload changed from a bare `userID` string to the full `GDPRRequest` (needed so `request_id` is available for `clearProcessed`).

**Temporal features demonstrated:**
- `SignalWithStartWorkflow` — atomic start-or-signal
- Signals + Query handler for live state inspection
- `ContinueAsNew`
- Temporal **Schedules**
- **Child workflows**
- Parallel activities, retries, heartbeat

### PII Protection

The ingest server receives the full deletion event (which may contain email, username, etc.) but only extracts `user_id`/`request_id`/`source` before forwarding to Temporal. PII fields are never serialized into the event history. `TestGDPRRequest_NoPIIFields` (in `workflows_test.go`) guards against a future change accidentally reintroducing a PII field into the type carried by the `addRequest` signal.

For stricter compliance, the signal payload can be reduced to `request_id` only (an opaque UUID), keeping the `user_id ↔ request_id` mapping outside of Temporal entirely.

---

## Production TODOs

| Item | Notes |
|---|---|
| Replace `InMemoryQueue` | Implement `Queue` interface backed by `aws-sdk-go-v2/service/sqs` |
| Real BQ client | `cloud.google.com/go/bigquery` — `UPDATE ... WHERE user_id IN (...)` |
| Real DynamoDB client | `aws-sdk-go-v2/service/dynamodb` — `BatchWriteItem` |
| Real SQS publish | `SendCompletionEvent` → `aws-sdk-go-v2/service/sqs` `SendMessage` |
| S3 history export | `ExportHistoryToS3` → `aws-sdk-go-v2/service/s3` `PutObject` |
| Temporal Cloud | Move from self-hosted to Temporal Cloud for managed infra |
| Worker fleet | Deploy multiple worker replicas behind a load balancer for horizontal scale |
| Production schedule interval | Switch `ProcessorInterval` from 1 minute (demo) to 5 minutes |
