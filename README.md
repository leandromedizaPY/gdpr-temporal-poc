# GDPR Anonymization Workflow — Temporal PoC

A proof of concept for orchestrating GDPR anonymization requests using [Temporal](https://temporal.io), built in Go.

## The Problem

When a GDPR deletion/anonymization request arrives, user data must be removed from **multiple systems** (BigQuery, DynamoDB, and potentially others). The current implementation using AWS Step Functions has two structural weaknesses:

- **No crash recovery** — if the process dies mid-execution, the partially-anonymized state is left with no automatic recovery. Someone has to manually figure out what ran and what didn't.
- **Scaling bottlenecks** — under high event volume, Step Functions struggles to keep up and becomes expensive.

## The Solution

Temporal's durable execution model solves both problems natively:

- **Crash recovery is automatic** — if the worker dies between the BigQuery step and the DynamoDB step, Temporal resumes exactly where it left off on restart. No manual intervention.
- **Horizontal scaling** — add more workers to handle more events concurrently. Temporal manages the queue.
- **Audit trail built-in** — the Temporal event history is an immutable, timestamped log of every step. This is the proof of completion for compliance purposes.

## Flow

```
SQS Event (BnD v2)
        │
        ▼
  GDPRWorkflow
   ↙           ↘
AnonymizeBQ   AnonymizeDynamo   ← runs in parallel
   ↘           ↙
 SendCompletionEvent → BnD v2
```

1. An SQS event from the BnD v2 team triggers a `GDPRWorkflow` for a given user.
2. BigQuery and DynamoDB anonymization run **in parallel**.
3. On completion, a response event is published back to BnD as confirmation.
4. The workflow ID is deterministic (`gdpr-{userID}`), so duplicate events are safely deduplicated.

## Temporal Features Used

| Feature | Where |
|---|---|
| **Parallel activities** | BQ and DynamoDB run simultaneously |
| **Retry policy** | Each activity retries automatically on failure |
| **Heartbeat** | BigQuery activity heartbeats to stay alive during long jobs |
| **Query handler** | Query `status` at any point to see live workflow progress |
| **Deterministic workflow ID** | `gdpr-{userID}` prevents double-processing |
| **Durable execution** | Worker crash mid-flow → resumes from last checkpoint |

## Project Structure

```
.
├── shared.go          # Types: GDPRRequest, GDPRResult, AnonymizationStatus
├── workflows.go       # GDPRWorkflow definition
├── activities.go      # AnonymizeBigQuery, AnonymizeDynamoDB, SendCompletionEvent
├── worker/main.go     # Worker process
├── starter/main.go    # Simulates an incoming SQS event from BnD
└── compose.yaml       # Docker Compose: Temporal server + worker + starter
```

## Running Locally

**Prerequisites:** Docker and Docker Compose.

```bash
# Start Temporal server + worker
docker compose up temporal worker

# In a separate terminal, trigger a workflow
docker compose run starter
```

The Temporal UI is available at [http://localhost:8080](http://localhost:8080) — you can watch the workflow execute step by step in real time.

**Without Docker:**

```bash
# Start Temporal dev server
temporal server start-dev

# In one terminal
go run ./worker

# In another terminal
go run ./starter
```

## Querying Workflow Status

While a workflow is running, you can query its live state:

```bash
temporal workflow query \
  --workflow-id gdpr-user-42 \
  --type status
```

## Current Limitations (PoC)

The activities are **mocked** — they simulate the real operations with sleeps and logs. To connect to real infrastructure, replace the bodies of:

- `AnonymizeBigQuery` → BigQuery client via `cloud.google.com/go/bigquery`
- `AnonymizeDynamoDB` → DynamoDB client via `github.com/aws/aws-sdk-go-v2/service/dynamodb`
- `SendCompletionEvent` → SQS publish via `github.com/aws/aws-sdk-go-v2/service/sqs`
