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

### Batch Collector Workflow (`GDPRBatchWorkflow`)

A long-running Temporal workflow that accumulates incoming `userID` signals and flushes them as a batch.

**Flush triggers (whichever fires first):**
- Batch reaches `maxBatchSize` (currently 10)
- Internal timer fires after `flushTimeout` (currently 10s)

**On flush:**
- `AnonymizeBigQueryBatch([]userID)` — one DML for all users in the batch
- `AnonymizeDynamoDBBatch([]userID)` — one `BatchWriteItem` for all users
- Both run in **parallel**

**History management:**
- `ContinueAsNew` after 100 batches to prevent unbounded history growth

**Temporal features demonstrated:**
- `SignalWithStartWorkflow` — atomic start-or-signal
- Durable timer — survives worker crashes
- Parallel activities
- `ContinueAsNew`
- Query handler for live state inspection

### PII Protection

The ingest server receives the full deletion event (which may contain email, username, etc.) but only extracts `userID` + `requestID` before forwarding to Temporal. PII fields are never serialized into the event history.

For stricter compliance, the signal payload can be reduced to `requestID` only (an opaque UUID), keeping the `userID ↔ requestID` mapping outside of Temporal entirely.

---

## Still to Build

### Scheduled Processor Workflow

Replace the internal flush timer with a **Temporal Schedule** that triggers a dedicated processor workflow on a fixed cadence (1 min demo / 5 min production).

**Architecture:**

```
Temporal Schedule (every 1 min)
  │
  ▼
GDPRProcessorWorkflow
  │  1. Query GDPRBatchWorkflow → get pending []GDPRRequest {userID, requestID}
  │  2. If empty → exit early
  │  3. AnonymizeBigQueryBatch([]userID)    ─┐ parallel
  │     AnonymizeDynamoDBBatch([]userID)    ─┘
  │  4. Signal GDPRBatchWorkflow("clearProcessed", []requestID)
  ▼
GDPRBatchWorkflow removes processed requestIDs from state
```

**Key design decision — use `requestID` not `userID` to clear state:**
New requests can arrive while the processor is running. Using `requestID` to identify what was processed ensures those new arrivals are not accidentally cleared. Each `requestID` is a UUID generated at ingest time, so it uniquely identifies a specific request regardless of `userID`.

**Changes required:**

- `workflows.go`
  - `GDPRBatchWorkflow`: add Query handler returning `[]GDPRRequest`, add `"clearProcessed"` signal handler accepting `[]string` (requestIDs), remove internal timer
  - New `GDPRProcessorWorkflow`: query → process → signal back

- `worker/main.go`: register `GDPRProcessorWorkflow`, create Temporal Schedule on startup

- `shared.go`: no changes needed

**Temporal features this adds:**
- Temporal Schedules
- Inter-workflow Query + Signal
- Early exit on empty batch

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
