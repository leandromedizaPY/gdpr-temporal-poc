package gdpr

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

type Activities struct{}

func NewActivities() (*Activities, error) {
	return &Activities{}, nil
}

// AnonymizeBigQuery simulates anonymizing a user's data in BigQuery.
// Uses heartbeat because BQ jobs can be long-running.
func (a *Activities) AnonymizeBigQuery(ctx context.Context, req GDPRRequest) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Anonymizing BigQuery data", "userID", req.UserID)

	// Simulate a BQ job that takes a few seconds
	for i := 0; i < 3; i++ {
		activity.RecordHeartbeat(ctx, fmt.Sprintf("bq step %d/3", i+1))
		time.Sleep(1 * time.Second)
	}

	logger.Info("BigQuery anonymization complete", "userID", req.UserID)
	return nil
}

// AnonymizeDynamoDB simulates anonymizing a user's data in DynamoDB.
func (a *Activities) AnonymizeDynamoDB(ctx context.Context, req GDPRRequest) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Anonymizing DynamoDB data", "userID", req.UserID)

	time.Sleep(500 * time.Millisecond)

	logger.Info("DynamoDB anonymization complete", "userID", req.UserID)
	return nil
}

// SendCompletionEvent simulates publishing a completion event back to BnD via SQS.
func (a *Activities) SendCompletionEvent(ctx context.Context, result GDPRResult) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Sending completion event to BnD",
		"userID", result.UserID,
		"requestID", result.RequestID,
		"status", result.Status,
	)

	// TODO: replace with real SQS publish via aws-sdk-go-v2
	time.Sleep(200 * time.Millisecond)

	logger.Info("Completion event sent", "userID", result.UserID)
	return nil
}
