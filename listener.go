package gdpr

import (
	"context"
	"encoding/json"
	"log"

	"go.temporal.io/sdk/client"
)

// Listener drains the input queue and signals the GDPRBatchWorkflow for each message.
// Only the userID is forwarded to Temporal — PII fields (email, username) in the
// original event are stripped at this layer and never reach the event history.
type Listener struct {
	temporalClient client.Client
	input          Queue
}

func NewListener(temporalClient client.Client, input Queue) *Listener {
	return &Listener{temporalClient: temporalClient, input: input}
}

func (l *Listener) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case body, ok := <-l.input.Receive():
			if !ok {
				return
			}
			l.handle(ctx, body)
		}
	}
}

const batchWorkflowID = "gdpr-batch-collector"

func (l *Listener) handle(ctx context.Context, body []byte) {
	var req GDPRRequest
	if err := json.Unmarshal(body, &req); err != nil {
		log.Printf("failed to decode GDPR request: %v", err)
		return
	}

	// SignalWithStartWorkflow atomically starts the batch workflow if not running,
	// then sends the signal. Only userID is sent — no PII reaches Temporal.
	_, err := l.temporalClient.SignalWithStartWorkflow(ctx,
		batchWorkflowID,
		"addRequest",
		req.UserID,
		client.StartWorkflowOptions{
			ID:        batchWorkflowID,
			TaskQueue: TaskQueue,
		},
		GDPRBatchWorkflow,
	)
	if err != nil {
		log.Printf("failed to signal batch workflow for user_id=%s: %v", req.UserID, err)
		return
	}

	log.Printf("queued user_id=%s into batch collector", req.UserID)
}
