package main

import (
	"context"
	"log"
	"net/http"

	gdpr "github.com/leandromedizaPY/gdpr-temporal-poc"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/worker"
)

const ingestAddr = ":8081"

func main() {
	c, err := client.Dial(envconfig.MustLoadDefaultClientOptions())
	if err != nil {
		log.Fatalln("Unable to create Temporal client", err)
	}
	defer c.Close()

	w := worker.New(c, gdpr.TaskQueue, worker.Options{})
	w.RegisterWorkflow(gdpr.GDPRWorkflow)

	activities, err := gdpr.NewActivities(c)
	if err != nil {
		log.Fatalln("Error creating activities", err)
	}
	w.RegisterActivity(activities)

	inputQueue := gdpr.NewInMemoryQueue(1024)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener := gdpr.NewListener(c, inputQueue)
	go listener.Run(ctx)

	server := &http.Server{Addr: ingestAddr, Handler: gdpr.NewIngestServer(inputQueue).Handler()}
	go func() {
		log.Printf("ingest endpoint listening on %s", ingestAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalln("ingest server failed", err)
		}
	}()
	defer server.Close()

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
