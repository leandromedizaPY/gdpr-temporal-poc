# GDPR Anonymization Workflow — Temporal PoC

A proof of concept for orchestrating GDPR anonymization requests using [Temporal](https://temporal.io), built in Go.

## The Problem

When a GDPR deletion/anonymization request arrives, user data must be removed from **multiple systems** (BigQuery, DynamoDB, and potentially others). The current implementation using AWS Step Functions has two structural weaknesses:

- **No crash recovery** — if the process dies mid-execution, the partially-anonymized state is left with no automatic recovery.
- **High cost at scale** — running one BQ query per user is expensive. Under high event volume, Step Functions also becomes a bottleneck.

## The Solution

Temporal's durable execution model solves both problems natively:

- **Crash recovery is automatic** — if the worker dies mid-execution, Temporal resumes exactly where it left off on restart. No manual intervention.
- **Batch processing** — requests are accumulated in a long-running workflow and flushed as a single BQ DML statement covering N users, instead of N individual queries.
- **Audit trail built-in** — the Temporal event history is an immutable, timestamped log of every step.

## Architecture

```
BnD v2
  │  (GDPR deletion event)
  ▼
HTTP :8081/events  (ingest server)
  │  strips PII — only userID + requestID forwarded
  ▼
InMemoryQueue  (swap point for real SQS)
  │
  ▼
Listener
  │  SignalWithStartWorkflow("addRequest", userID)
  ▼
GDPRBatchWorkflow  (long-running collector)
  │  accumulates userIDs via signals
  │  flushes when batch = 10 OR timer = 10s
  ▼
  ├── AnonymizeBigQueryBatch([]userID)   ─┐ parallel
  └── AnonymizeDynamoDBBatch([]userID)   ─┘ one DML per batch
```

### Next step (see PLAN.md)

Replace the internal flush timer with a **Temporal Schedule** that triggers a dedicated processor workflow. The processor queries the collector for pending requests, processes them, and signals back with the processed `requestIDs` to clear them from state.

## Temporal Features Used

| Feature | Where |
|---|---|
| **Signals** | Each incoming event is delivered to the batch collector via `SignalWithStartWorkflow` |
| **Long-running workflow** | `GDPRBatchWorkflow` stays alive indefinitely, accumulating requests |
| **Durable timers** | Flush timer survives worker crashes and resumes automatically |
| **ContinueAsNew** | Resets event history after 100 batches to keep it bounded |
| **Parallel activities** | BQ and DynamoDB batch operations run simultaneously |
| **Retry policy** | Each activity retries automatically on failure |
| **Heartbeat** | BigQuery activity heartbeats during long batch jobs |
| **Query handler** | Query live workflow state without interrupting execution |
| **Durable execution** | Worker crash mid-flow → resumes from last checkpoint |

## PII Protection

The full deletion event (email, username, etc.) only lives in memory inside the ingest server. Only the `userID` is forwarded to Temporal via signal — **PII fields never reach the event history**.

For stricter compliance, the signal payload can be changed to use only the `requestID` (an opaque UUID), keeping the `userID` mapping entirely outside of Temporal.

## Project Structure

```
.
├── shared.go          # Types: GDPRRequest, GDPRResult, AnonymizationStatus
├── workflows.go       # GDPRBatchWorkflow (batch collector) + GDPRWorkflow (per-event, reference)
├── activities.go      # AnonymizeBigQueryBatch, AnonymizeDynamoDBBatch, SendCompletionEvent, ExportHistoryToS3
├── queue.go           # Queue interface + InMemoryQueue (swap point for SQS)
├── ingest.go          # HTTP server on :8081 — receives events and enqueues them
├── listener.go        # Drains queue, signals GDPRBatchWorkflow per message
├── worker/main.go     # Worker process — registers workflows, activities, ingest server, listener
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

The Temporal UI is at [http://localhost:8080](http://localhost:8080). You'll see the `gdpr-batch-collector` workflow accumulate signals and flush the batch after 10 seconds.

**Without Docker:**

```bash
# Terminal 1 — Temporal dev server
temporal server start-dev

# Terminal 2 — worker + ingest server
TEMPORAL_ADDRESS=localhost:7233 go run ./worker

# Terminal 3 — send events
go run ./producer -count=5
```

## Querying Live Batch State

```bash
temporal workflow query \
  --workflow-id gdpr-batch-collector \
  --type status
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
