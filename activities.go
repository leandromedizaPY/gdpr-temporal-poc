package gdpr

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/encoding/protojson"
)

type Activities struct {
	temporalClient client.Client
}

func NewActivities(temporalClient client.Client) (*Activities, error) {
	return &Activities{temporalClient: temporalClient}, nil
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

// ExportHistoryToS3 fetches the full workflow execution history from Temporal
// and writes it as JSON — the immutable audit trail proving every step ran.
// TODO: replace local file write with aws-sdk-go-v2 S3 PutObject.
func (a *Activities) ExportHistoryToS3(ctx context.Context, workflowID, runID string) (string, error) {
	logger := activity.GetLogger(ctx)

	iter := a.temporalClient.GetWorkflowHistory(
		ctx, workflowID, runID, false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)

	marshaler := protojson.MarshalOptions{EmitUnpopulated: false}
	var events []json.RawMessage
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return "", fmt.Errorf("reading history: %w", err)
		}
		b, err := marshaler.Marshal(event)
		if err != nil {
			return "", fmt.Errorf("marshaling event: %w", err)
		}
		events = append(events, b)
	}

	payload, err := json.MarshalIndent(map[string]any{
		"workflow_id": workflowID,
		"run_id":      runID,
		"event_count": len(events),
		"events":      events,
	}, "", "  ")
	if err != nil {
		return "", err
	}

	// S3 key shape: gdpr-history/{workflowID}/{runID}.json
	// TODO: swap for s3Client.PutObject(ctx, &s3.PutObjectInput{Bucket: ..., Key: key, Body: ...})
	localPath := filepath.Join("history", workflowID, runID+".json")
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(localPath, payload, 0o644); err != nil {
		return "", err
	}

	s3Key := fmt.Sprintf("gdpr-history/%s/%s.json", workflowID, runID)
	logger.Info("Workflow history exported", "path", localPath, "events", len(events), "s3Key", s3Key)
	return s3Key, nil
}
