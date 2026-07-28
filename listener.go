package gdpr

import (
	"context"
	"encoding/json"
	"log"

	"go.temporal.io/sdk/client"
)

// Listener drains the input queue and signals GDPRBatchCollectorWorkflow for
// each message. Only user_id/request_id/source are forwarded to Temporal —
// PII fields (email, username) in the original event are stripped at the
// ingest layer and never reach the event history.
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

func (l *Listener) handle(ctx context.Context, body []byte) {
	var req GDPRRequest
	if err := json.Unmarshal(body, &req); err != nil {
		log.Printf("failed to decode GDPR request: %v", err)
		return
	}

	// SignalWithStartWorkflow atomically starts the collector if not
	// already running, then delivers the signal. Only
	// user_id/request_id/source are sent — no PII reaches Temporal. The
	// []GDPRRequest(nil) argument is the collector's initial pending state,
	// only used the first time it's started — ignored when the signal is
	// delivered to an already-running collector.
	_, err := l.temporalClient.SignalWithStartWorkflow(ctx,
		CollectorWorkflowID,
		SignalAddRequest,
		req,
		client.StartWorkflowOptions{
			ID:        CollectorWorkflowID,
			TaskQueue: TaskQueue,
		},
		GDPRBatchCollectorWorkflow,
		[]GDPRRequest(nil),
	)
	if err != nil {
		log.Printf("failed to signal batch collector for request_id=%s: %v", req.RequestID, err)
		return
	}

	log.Printf("queued user_id=%s request_id=%s into batch collector", req.UserID, req.RequestID)
}
