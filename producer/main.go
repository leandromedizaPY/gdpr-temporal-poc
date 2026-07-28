package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	gdpr "github.com/leandromedizaPY/gdpr-temporal-poc"
	"github.com/google/uuid"
)

const defaultIngestURL = "http://localhost:8081/events"

// producer is a stub for a real upstream publisher (e.g. BnD v2).
// It POSTs synthetic GDPR deletion requests to the worker's ingest endpoint.
func main() {
	count := flag.Int("count", 1, "number of deletion request messages to publish")
	flag.Parse()

	ingestURL := os.Getenv("INGEST_URL")
	if ingestURL == "" {
		ingestURL = defaultIngestURL
	}

	for i := 0; i < *count; i++ {
		req := gdpr.GDPRRequest{
			UserID:    fmt.Sprintf("user-%d", i+1),
			RequestID: uuid.NewString(),
			Source:    "bnd-v2",
		}

		body, err := json.Marshal(req)
		if err != nil {
			log.Fatalln("failed to marshal request", err)
		}

		resp, err := http.Post(ingestURL, "application/json", bytes.NewReader(body))
		if err != nil {
			log.Fatalln("failed to publish event", err)
		}
		resp.Body.Close()

		log.Printf("published user_id=%s request_id=%s status=%d", req.UserID, req.RequestID, resp.StatusCode)

		if i < *count-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}
