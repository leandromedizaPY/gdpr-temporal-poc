package gdpr

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// CollectorWorkflowID is the fixed, singleton Workflow ID for
// GDPRBatchCollectorWorkflow. SignalWithStartWorkflow always targets this ID.
const CollectorWorkflowID = "gdpr-batch-collector"

// Signal and query names for GDPRBatchCollectorWorkflow.
const (
	SignalAddRequest     = "addRequest"
	SignalClearProcessed = "clearProcessed"
	QueryPendingRequests = "pendingRequests"
)

// maxCollectorCyclesBeforeContinueAsNew bounds the collector's event history,
// since it no longer resets on an internal flush timer — clearing now only
// happens via an external "clearProcessed" signal from GDPRSchedulerWorkflow.
const maxCollectorCyclesBeforeContinueAsNew = 500

// GDPRBatchCollectorWorkflow is the long-running, singleton accumulator of
// pending GDPR deletion requests (Workflow ID = CollectorWorkflowID). It
// never calls BigQuery/DynamoDB — its only job is to hold pending
// {user_id, request_id} pairs so GDPRSchedulerWorkflow can drain it on a
// schedule. A duplicate "addRequest" signal for a request_id already
// pending is silently ignored — this is the idempotency mechanism now that
// there's no longer one workflow per user.
func GDPRBatchCollectorWorkflow(ctx workflow.Context, initial []GDPRRequest) error {
	logger := workflow.GetLogger(ctx)

	pending := make(map[string]GDPRRequest, len(initial))
	for _, req := range initial {
		pending[req.RequestID] = req
	}

	err := workflow.SetQueryHandler(ctx, QueryPendingRequests, func() ([]GDPRRequest, error) {
		return pendingList(pending), nil
	})
	if err != nil {
		return err
	}

	addCh := workflow.GetSignalChannel(ctx, SignalAddRequest)
	clearCh := workflow.GetSignalChannel(ctx, SignalClearProcessed)
	cycles := 0

	for {
		selector := workflow.NewSelector(ctx)

		selector.AddReceive(addCh, func(c workflow.ReceiveChannel, _ bool) {
			var req GDPRRequest
			c.Receive(ctx, &req)
			if _, exists := pending[req.RequestID]; exists {
				logger.Info("duplicate GDPR request while pending, ignoring", "requestID", req.RequestID)
				return
			}
			pending[req.RequestID] = req
			logger.Info("GDPR request queued", "userID", req.UserID, "requestID", req.RequestID, "pendingCount", len(pending))
		})

		selector.AddReceive(clearCh, func(c workflow.ReceiveChannel, _ bool) {
			var requestIDs []string
			c.Receive(ctx, &requestIDs)
			for _, id := range requestIDs {
				delete(pending, id)
			}
			logger.Info("cleared processed GDPR requests", "count", len(requestIDs), "pendingCount", len(pending))
		})

		selector.Select(ctx)
		cycles++

		if cycles >= maxCollectorCyclesBeforeContinueAsNew {
			logger.Info("continuing collector as new", "pendingCount", len(pending), "cycles", cycles)
			return workflow.NewContinueAsNewError(ctx, GDPRBatchCollectorWorkflow, pendingList(pending))
		}
	}
}

func pendingList(pending map[string]GDPRRequest) []GDPRRequest {
	out := make([]GDPRRequest, 0, len(pending))
	for _, req := range pending {
		out = append(out, req)
	}
	return out
}

// GDPRSchedulerWorkflow runs once per Temporal Schedule tick
// (ProcessorInterval, see schedule.go). It queries the collector for
// pending requests and, if any exist, runs them through
// GDPRProcessorWorkflow as a child workflow before signaling the collector
// to clear exactly the request_ids that were processed.
func GDPRSchedulerWorkflow(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)

	queryOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	ctx = workflow.WithActivityOptions(ctx, queryOpts)

	var a *Activities

	var batch []GDPRRequest
	if err := workflow.ExecuteActivity(ctx, a.QueryCollectorPending).Get(ctx, &batch); err != nil {
		return fmt.Errorf("query collector pending: %w", err)
	}

	if len(batch) == 0 {
		logger.Info("no pending GDPR requests, skipping this tick")
		return nil
	}

	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		TaskQueue: TaskQueue,
	})

	if err := workflow.ExecuteChildWorkflow(childCtx, GDPRProcessorWorkflow, batch).Get(ctx, nil); err != nil {
		// Leave the batch pending — never cleared — so it's retried on the
		// next scheduled tick instead of being silently lost.
		logger.Error("processor child workflow failed, leaving batch pending for retry", "error", err)
		return err
	}

	// Only reachable once the processor durably succeeded — safe to clear
	// exactly this snapshot's request_ids. Any addRequest signals that
	// landed on the collector after the query above are untouched, since
	// only the request_ids captured here are named — never a blanket clear.
	requestIDs := make([]string, len(batch))
	for i, req := range batch {
		requestIDs[i] = req.RequestID
	}
	if err := workflow.SignalExternalWorkflow(ctx, CollectorWorkflowID, "", SignalClearProcessed, requestIDs).Get(ctx, nil); err != nil {
		logger.Error("failed to signal collector to clear processed requests", "error", err)
		return err
	}

	logger.Info("batch processed and cleared", "count", len(batch))
	return nil
}

// GDPRProcessorWorkflow runs the actual (batched) anonymization work for one
// scheduled tick's worth of pending requests, as a child of
// GDPRSchedulerWorkflow. One BQ call and one DynamoDB call cover the whole
// batch, instead of one per request.
func GDPRProcessorWorkflow(ctx workflow.Context, batch []GDPRRequest) error {
	logger := workflow.GetLogger(ctx)

	userIDs := make([]string, len(batch))
	for i, req := range batch {
		userIDs[i] = req.UserID
	}

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
	bqFuture := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, bqOpts), a.AnonymizeBigQueryBatch, userIDs)
	dynamoFuture := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, dynamoOpts), a.AnonymizeDynamoDBBatch, userIDs)

	bqErr := bqFuture.Get(ctx, nil)
	dynamoErr := dynamoFuture.Get(ctx, nil)

	if bqErr != nil || dynamoErr != nil {
		return fmt.Errorf("batch anonymization failed: bq=%v dynamo=%v", bqErr, dynamoErr)
	}

	eventOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	}
	eventCtx := workflow.WithActivityOptions(ctx, eventOpts)
	for _, req := range batch {
		result := GDPRResult{UserID: req.UserID, RequestID: req.RequestID, Status: "completed", BQDone: true, DynamoDone: true}
		if err := workflow.ExecuteActivity(eventCtx, a.SendCompletionEvent, result).Get(ctx, nil); err != nil {
			logger.Error("failed to send completion event", "userID", req.UserID, "error", err)
		}
	}

	exportOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	info := workflow.GetInfo(ctx)
	var s3Key string
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, exportOpts), a.ExportHistoryToS3,
		info.WorkflowExecution.ID, info.WorkflowExecution.RunID,
	).Get(ctx, &s3Key); err != nil {
		logger.Error("history export failed", "error", err)
	} else {
		logger.Info("history exported", "s3Key", s3Key)
	}

	logger.Info("batch processing complete", "users", len(userIDs))
	return nil
}

// GDPRWorkflow is kept for reference — the production flow uses
// GDPRBatchCollectorWorkflow + GDPRSchedulerWorkflow + GDPRProcessorWorkflow.
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
		UserID:     req.UserID,
		RequestID:  req.RequestID,
		Status:     "completed",
		BQDone:     true,
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
