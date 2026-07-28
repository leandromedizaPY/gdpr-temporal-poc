package gdpr

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func GDPRWorkflow(ctx workflow.Context, req GDPRRequest) (GDPRResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("GDPR workflow started", "userID", req.UserID, "requestID", req.RequestID)

	status := AnonymizationStatus{UserID: req.UserID}

	// Query handler — lets anyone ask "what's the current state?" without interrupting the workflow
	if err := workflow.SetQueryHandler(ctx, "status", func() (AnonymizationStatus, error) {
		return status, nil
	}); err != nil {
		return GDPRResult{}, err
	}

	bqOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		HeartbeatTimeout:    10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}

	dynamoOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}

	var a *Activities

	// Fan-out: run BQ and DynamoDB anonymization in parallel
	bqCtx := workflow.WithActivityOptions(ctx, bqOpts)
	dynamoCtx := workflow.WithActivityOptions(ctx, dynamoOpts)

	bqFuture := workflow.ExecuteActivity(bqCtx, a.AnonymizeBigQuery, req)
	dynamoFuture := workflow.ExecuteActivity(dynamoCtx, a.AnonymizeDynamoDB, req)

	// Wait for both — collect errors independently so one failure doesn't mask the other
	bqErr := bqFuture.Get(ctx, nil)
	if bqErr == nil {
		status.BQDone = true
	}

	dynamoErr := dynamoFuture.Get(ctx, nil)
	if dynamoErr == nil {
		status.DynamoDone = true
	}

	if bqErr != nil || dynamoErr != nil {
		return GDPRResult{
			UserID:     req.UserID,
			RequestID:  req.RequestID,
			Status:     "failed",
			BQDone:     status.BQDone,
			DynamoDone: status.DynamoDone,
		}, fmt.Errorf("anonymization failed: bq=%v dynamo=%v", bqErr, dynamoErr)
	}

	// Send completion event back to BnD
	result := GDPRResult{
		UserID:     req.UserID,
		RequestID:  req.RequestID,
		Status:     "completed",
		BQDone:     true,
		DynamoDone: true,
	}

	eventOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
		},
	}
	eventCtx := workflow.WithActivityOptions(ctx, eventOpts)

	if err := workflow.ExecuteActivity(eventCtx, a.SendCompletionEvent, result).Get(ctx, nil); err != nil {
		logger.Error("Failed to send completion event", "error", err)
		return result, err
	}

	status.EventSent = true
	logger.Info("GDPR workflow completed", "userID", req.UserID)
	return result, nil
}
