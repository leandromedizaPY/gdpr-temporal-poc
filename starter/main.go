package main

import (
	"context"
	"fmt"
	"log"

	gdpr "github.com/leandromedizaPY/gdpr-temporal-poc"
	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
)

func main() {
	c, err := client.Dial(envconfig.MustLoadDefaultClientOptions())
	if err != nil {
		log.Fatalln("Unable to create Temporal client", err)
	}
	defer c.Close()

	// Simulate an SQS event from BnD v2
	req := gdpr.GDPRRequest{
		UserID:    "user-42",
		RequestID: uuid.NewString(),
		Source:    "bnd-v2",
	}

	// Workflow ID is deterministic per user — prevents double-processing the same request
	workflowID := fmt.Sprintf("gdpr-%s", req.UserID)

	options := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: gdpr.TaskQueue,
	}

	we, err := c.ExecuteWorkflow(context.Background(), options, gdpr.GDPRWorkflow, req)
	if err != nil {
		log.Fatalln("Unable to start workflow", err)
	}

	log.Printf("Started workflow workflowID=%s runID=%s", we.GetID(), we.GetRunID())

	var result gdpr.GDPRResult
	if err := we.Get(context.Background(), &result); err != nil {
		log.Fatalln("Workflow failed", err)
	}

	log.Printf("Workflow completed: userID=%s status=%s bq=%v dynamo=%v",
		result.UserID, result.Status, result.BQDone, result.DynamoDone)
}
