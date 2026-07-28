package gdpr

import (
	"context"
	"encoding/json"
	"log"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// Listener drains the input queue and starts one GDPRWorkflow per message.
// Workflow ID = "gdpr-{userID}" so a duplicate message can't start a second run —
// Temporal rejects it instead of reprocessing.
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

	_, err := l.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    "gdpr-" + req.UserID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}, GDPRWorkflow, req)
	if err != nil {
		log.Printf("failed to start workflow for user_id=%s: %v", req.UserID, err)
		return
	}

	log.Printf("started workflow for user_id=%s request_id=%s", req.UserID, req.RequestID)
}
