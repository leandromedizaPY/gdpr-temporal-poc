package gdpr

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

// TestGDPRBatchCollectorWorkflow_DedupAndExactIDClear proves the two
// idempotency properties the batch collector is meant to give us instead of
// a per-user Workflow ID: a duplicate addRequest signal for a request_id
// already pending is a no-op, and clearProcessed removes only the named
// request_ids, never a blanket clear — this is what protects a request
// that arrives between a scheduler tick's query and its clear signal.
func TestGDPRBatchCollectorWorkflow_DedupAndExactIDClear(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	reqA := GDPRRequest{UserID: "user-1", RequestID: "req-1", Source: "bnd-v2"}
	reqB := GDPRRequest{UserID: "user-2", RequestID: "req-2", Source: "bnd-v2"}

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalAddRequest, reqA)
	}, time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalAddRequest, reqA) // duplicate request_id — must be ignored
	}, 2*time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalAddRequest, reqB)
	}, 3*time.Millisecond)

	env.RegisterDelayedCallback(func() {
		result, err := env.QueryWorkflow(QueryPendingRequests)
		require.NoError(t, err)
		var pending []GDPRRequest
		require.NoError(t, result.Get(&pending))
		require.Len(t, pending, 2, "duplicate addRequest must not create a second pending entry")
	}, 4*time.Millisecond)

	// Simulate a scheduler tick clearing only req-1 — proves req-2 (which
	// could represent a request that arrived after the tick's query
	// snapshot) survives untouched.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalClearProcessed, []string{reqA.RequestID})
	}, 5*time.Millisecond)

	env.RegisterDelayedCallback(func() {
		result, err := env.QueryWorkflow(QueryPendingRequests)
		require.NoError(t, err)
		var pending []GDPRRequest
		require.NoError(t, result.Get(&pending))
		require.Len(t, pending, 1)
		require.Equal(t, reqB.RequestID, pending[0].RequestID,
			"clearProcessed must remove only the named request_id, leaving unrelated pending requests untouched")
	}, 6*time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 7*time.Millisecond)

	env.ExecuteWorkflow(GDPRBatchCollectorWorkflow, []GDPRRequest(nil))

	require.True(t, env.IsWorkflowCompleted())
}

// TestGDPRRequest_NoPIIFields guards against a future change accidentally
// reintroducing PII into the type carried by the addRequest signal — the
// only fields allowed are non-PII identifiers/metadata.
func TestGDPRRequest_NoPIIFields(t *testing.T) {
	allowed := map[string]bool{
		"UserID":    true,
		"RequestID": true,
		"Source":    true,
	}

	typ := reflect.TypeOf(GDPRRequest{})
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		require.True(t, allowed[name],
			"GDPRRequest has an unexpected field %q — if this is PII (email, username, etc.), "+
				"it must never be signaled into GDPRBatchCollectorWorkflow; strip it in ingest.go instead", name)
	}
}
