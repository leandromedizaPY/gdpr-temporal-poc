package main

import (
	"log"

	gdpr "github.com/leandromedizaPY/gdpr-temporal-poc"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(envconfig.MustLoadDefaultClientOptions())
	if err != nil {
		log.Fatalln("Unable to create Temporal client", err)
	}
	defer c.Close()

	w := worker.New(c, gdpr.TaskQueue, worker.Options{})

	w.RegisterWorkflow(gdpr.GDPRWorkflow)

	activities, err := gdpr.NewActivities()
	if err != nil {
		log.Fatalln("Error creating activities", err)
	}
	w.RegisterActivity(activities)

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
