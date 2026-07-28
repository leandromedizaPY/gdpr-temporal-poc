package gdpr

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// GDPRBatchWorkflow is a long-running workflow that collects userIDs via signals
// and flushes them as a batch when either maxBatchSize is reached or flushTimeout fires.
//
// Signal name: "addRequest", payload: userID string
//
// Only the userID (an internal identifier) is ever sent to Temporal —
// PII fields (email, username) stay in the ingest layer and never reach the event history.
func GDPRBatchWorkflow(ctx workflow.Context) error {
	const (
		maxBatchSize  = 10
		flushTimeout  = 10 * time.Second
		maxBatches    = 100 // ContinueAsNew after this many batches to keep history bounded
	)

	logger := workflow.GetLogger(ctx)
	addCh := workflow.GetSignalChannel(ctx, "addRequest")
	batchCount := 0

	for {
		// Wait for the first userID before starting the flush timer
		var userID string
		addCh.Receive(ctx, &userID)
		pending := []string{userID}

		// Start flush timer now that we have at least one item
		timerCtx, cancelTimer := workflow.WithCancel(ctx)
		timer := workflow.NewTimer(timerCtx, flushTimeout)
		timerFired := false

		for !timerFired && len(pending) < maxBatchSize {
			sel := workflow.NewSelector(ctx)
			sel.AddReceive(addCh, func(c workflow.ReceiveChannel, _ bool) {
				var id string
				c.Receive(ctx, &id)
				pending = append(pending, id)
			})
			sel.AddFuture(timer, func(_ workflow.Future) {
				timerFired = true
			})
			sel.Select(ctx)
		}
		cancelTimer()

		logger.Info("flushing batch", "size", len(pending), "timerFired", timerFired)

		bqOpts := workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Minute,
			HeartbeatTimeout:    15 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
		}
		dynamoOpts := workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
		}

		var a *Activities
		bqFuture := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, bqOpts), a.AnonymizeBigQueryBatch, pending)
		dynamoFuture := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, dynamoOpts), a.AnonymizeDynamoDBBatch, pending)

		bqErr := bqFuture.Get(ctx, nil)
		dynamoErr := dynamoFuture.Get(ctx, nil)

		if bqErr != nil || dynamoErr != nil {
			logger.Error("batch anonymization failed", "bq", bqErr, "dynamo", dynamoErr)
		} else {
			logger.Info("batch completed successfully", "size", len(pending))
		}

		batchCount++
		// ContinueAsNew resets the event history to prevent it from growing unbounded.
		// Pending signals are carried over automatically.
		if batchCount >= maxBatches {
			return workflow.NewContinueAsNewError(ctx, GDPRBatchWorkflow)
		}
	}
}

// GDPRWorkflow is kept for reference — the production flow uses GDPRBatchWorkflow.
func GDPRWorkflow(ctx workflow.Context, req GDPRRequest) (GDPRResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("GDPR workflow started", "userID", req.UserID, "requestID", req.RequestID)

	status := AnonymizationStatus{UserID: req.UserID}

	if err := workflow.SetQueryHandler(ctx, "status", func() (AnonymizationStatus, error) {
		return status, nil
	}); err != nil {
		return GDPRResult{}, err
	}

	bqOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		HeartbeatTimeout:    10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	dynamoOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}

	var a *Activities

	bqFuture := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, bqOpts), a.AnonymizeBigQuery, req)
	dynamoFuture := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, dynamoOpts), a.AnonymizeDynamoDB, req)

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

	result := GDPRResult{
		UserID:    req.UserID,
		RequestID: req.RequestID,
		Status:    "completed",
		BQDone:    true,
		DynamoDone: true,
	}

	eventOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	}
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, eventOpts), a.SendCompletionEvent, result).Get(ctx, nil); err != nil {
		logger.Error("Failed to send completion event", "error", err)
		return result, err
	}
	status.EventSent = true

	exportOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	info := workflow.GetInfo(ctx)
	var s3Key string
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, exportOpts), a.ExportHistoryToS3,
		info.WorkflowExecution.ID,
		info.WorkflowExecution.RunID,
	).Get(ctx, &s3Key); err != nil {
		logger.Error("History export failed", "error", err)
	} else {
		logger.Info("History exported", "s3Key", s3Key)
	}

	logger.Info("GDPR workflow completed", "userID", req.UserID)
	return result, nil
}
