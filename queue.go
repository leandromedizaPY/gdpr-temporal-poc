package gdpr

import "context"

// Queue is a minimal message queue abstraction standing in for a real SQS queue.
// Swapping in an aws-sdk-go-v2-backed implementation is an isolated change behind this interface.
type Queue interface {
	Send(ctx context.Context, body []byte) error
	Receive() <-chan []byte
}

// InMemoryQueue is a buffered-channel Queue used for local runs.
type InMemoryQueue struct {
	ch chan []byte
}

func NewInMemoryQueue(buffer int) *InMemoryQueue {
	return &InMemoryQueue{ch: make(chan []byte, buffer)}
}

func (q *InMemoryQueue) Send(ctx context.Context, body []byte) error {
	select {
	case q.ch <- body:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *InMemoryQueue) Receive() <-chan []byte {
	return q.ch
}
