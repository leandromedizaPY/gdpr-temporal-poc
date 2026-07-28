package gdpr

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// SchedulerScheduleID is the fixed ID of the Temporal Schedule that drives
// GDPRSchedulerWorkflow.
const SchedulerScheduleID = "gdpr-scheduler-schedule"

// ProcessorInterval controls how often the Schedule fires. 1 minute is a
// demo-friendly cadence; production would likely use 5 minutes instead.
const ProcessorInterval = 1 * time.Minute

// EnsureSchedulerSchedule idempotently creates the Temporal Schedule that
// triggers GDPRSchedulerWorkflow, tolerating "already exists" so it's safe
// to call on every worker startup.
func EnsureSchedulerSchedule(ctx context.Context, c client.Client) error {
	_, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID: SchedulerScheduleID,
		Spec: client.ScheduleSpec{
			Intervals: []client.ScheduleIntervalSpec{{Every: ProcessorInterval}},
		},
		Action: &client.ScheduleWorkflowAction{
			Workflow:  GDPRSchedulerWorkflow,
			TaskQueue: TaskQueue,
		},
	})
	if err != nil {
		if errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
			return nil
		}
		return err
	}
	return nil
}
