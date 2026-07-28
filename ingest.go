package gdpr

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
)

// IngestServer exposes a local HTTP endpoint the producer posts to,
// bridging into the worker process's in-memory input queue.
// Stands in for publishing to a real SQS queue.
type IngestServer struct {
	input Queue
}

func NewIngestServer(input Queue) *IngestServer {
	return &IngestServer{input: input}
}

func (s *IngestServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/events", s.handleEvent)
	return mux
}

func (s *IngestServer) handleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var req GDPRRequest
	if err := json.Unmarshal(body, &req); err != nil || req.UserID == "" || req.RequestID == "" {
		http.Error(w, "invalid payload: user_id and request_id are required", http.StatusBadRequest)
		return
	}

	if err := s.input.Send(r.Context(), body); err != nil {
		http.Error(w, "failed to enqueue event: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("ingested user_id=%s request_id=%s", req.UserID, req.RequestID)
	w.WriteHeader(http.StatusAccepted)
}
