package gdpr

const TaskQueue = "gdpr-task-queue"

// GDPRRequest represents an anonymization request received from BnD via SQS.
type GDPRRequest struct {
	UserID    string `json:"user_id"`
	RequestID string `json:"request_id"` // idempotency key
	Source    string `json:"source"`     // e.g. "bnd-v2"
}

// GDPRResult is returned when the workflow completes and sent back to BnD.
type GDPRResult struct {
	UserID     string `json:"user_id"`
	RequestID  string `json:"request_id"`
	Status     string `json:"status"` // "completed" | "failed"
	BQDone     bool   `json:"bq_done"`
	DynamoDone bool   `json:"dynamo_done"`
}

// AnonymizationStatus is exposed via Query to show live workflow progress.
type AnonymizationStatus struct {
	UserID     string `json:"user_id"`
	BQDone     bool   `json:"bq_done"`
	DynamoDone bool   `json:"dynamo_done"`
	EventSent  bool   `json:"event_sent"`
}
