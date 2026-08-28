package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
)

// scheduleRecorder hands out nodes in order and records the exclusions the
// gateway asked for on each attempt.
type scheduleRecorder struct {
	mu         sync.Mutex
	nodes      []string
	calls      int
	exclusions [][]string
}

func (s *scheduleRecorder) next(_ context.Context, req *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exclusions = append(s.exclusions, req.GetExcludeNodeIds())
	if s.calls >= len(s.nodes) {
		return nil, fmt.Errorf("no nodes available")
	}
	nodeID := s.nodes[s.calls]
	s.calls++
	return &schedulerv1.ScheduleResponse{
		Node: &schedulerv1.Node{NodeId: nodeID, Endpoint: "http://" + nodeID},
	}, nil
}

// TestScheduledCreateRetriesOnNodeRejection covers the half of admission
// control that makes it safe: a node refusing a create must steer the
// placement, not surface as a client-visible failure.
func TestScheduledCreateRetriesOnNodeRejection(t *testing.T) {
	var refused []string
	var mu sync.Mutex

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		// The first two nodes are at capacity; the third accepts.
		if len(refused) < 2 {
			refused = append(refused, r.Host)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("node at capacity (sandbox_count); retry after 2s"))
			return
		}
		w.Header().Set(headerSandboxID, "sbx-accepted")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sandboxID":"sbx-accepted"}`))
	}))
	defer upstream.Close()

	recorder := &scheduleRecorder{nodes: []string{"node-a", "node-b", "node-c"}}
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(ctx context.Context, req *schedulerv1.ScheduleRequest, opts ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			resp, err := recorder.next(ctx, req, opts...)
			if err != nil {
				return nil, err
			}
			// Point every placement at the one test upstream.
			resp.Node.Endpoint = upstream.URL
			return resp, nil
		},
		recordAssignmentFunc: func(_ context.Context, _ *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, 5*time.Second, 1<<20)

	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 after rescheduling past two full nodes; body=%q", rec.Code, rec.Body.String())
	}

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.calls != 3 {
		t.Fatalf("Schedule called %d times, want 3", recorder.calls)
	}
	// Each retry must tell the scheduler which nodes already refused, or it
	// would hand back the same node and the loop would make no progress.
	if len(recorder.exclusions) != 3 {
		t.Fatalf("recorded %d schedule calls, want 3", len(recorder.exclusions))
	}
	if len(recorder.exclusions[0]) != 0 {
		t.Fatalf("first attempt excluded %v, want none", recorder.exclusions[0])
	}
	if len(recorder.exclusions[1]) != 1 || recorder.exclusions[1][0] != "node-a" {
		t.Fatalf("second attempt excluded %v, want [node-a]", recorder.exclusions[1])
	}
	if len(recorder.exclusions[2]) != 2 {
		t.Fatalf("third attempt excluded %v, want both refusing nodes", recorder.exclusions[2])
	}
}

// TestScheduledCreateStopsRetryingAtTheBound pins that a saturated fleet does
// not turn every create into a walk of the whole node list. The last attempt's
// rejection is returned to the client.
func TestScheduledCreateStopsRetryingAtTheBound(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("node at capacity (sandbox_count); retry after 2s"))
	}))
	defer upstream.Close()

	recorder := &scheduleRecorder{nodes: []string{"a", "b", "c", "d", "e", "f"}}
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(ctx context.Context, req *schedulerv1.ScheduleRequest, opts ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			resp, err := recorder.next(ctx, req, opts...)
			if err != nil {
				return nil, err
			}
			resp.Node.Endpoint = upstream.URL
			return resp, nil
		},
	}, 5*time.Second, 1<<20)

	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want the final rejection surfaced as 503", rec.Code)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.calls != maxScheduleAttempts {
		t.Fatalf("Schedule called %d times, want it bounded at %d", recorder.calls, maxScheduleAttempts)
	}
}

// A non-503 answer is the node's real answer about the request and must reach
// the client unchanged rather than being retried elsewhere.
func TestScheduledCreateDoesNotRetryNonRejections(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad template"))
	}))
	defer upstream.Close()

	recorder := &scheduleRecorder{nodes: []string{"a", "b", "c"}}
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(ctx context.Context, req *schedulerv1.ScheduleRequest, opts ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			resp, err := recorder.next(ctx, req, opts...)
			if err != nil {
				return nil, err
			}
			resp.Node.Endpoint = upstream.URL
			return resp, nil
		},
	}, 5*time.Second, 1<<20)

	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want the node's own 400 passed through", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "bad template") {
		t.Fatalf("body = %q, want the upstream body preserved", body)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.calls != 1 {
		t.Fatalf("Schedule called %d times, want no retry on a non-rejection", recorder.calls)
	}
}

// The retried request must carry the original body; a truncated create would
// be worse than not retrying at all.
func TestScheduledCreateReplaysTheRequestBody(t *testing.T) {
	const body = `{"templateID":"tpl","metadata":{"k":"v"}}`
	var seen []string
	var mu sync.Mutex

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received, _ := io.ReadAll(r.Body)
		mu.Lock()
		seen = append(seen, string(received))
		attempt := len(seen)
		mu.Unlock()

		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set(headerSandboxID, "sbx-1")
		w.WriteHeader(http.StatusCreated)
	}))
	defer upstream.Close()

	recorder := &scheduleRecorder{nodes: []string{"a", "b"}}
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(ctx context.Context, req *schedulerv1.ScheduleRequest, opts ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			resp, err := recorder.next(ctx, req, opts...)
			if err != nil {
				return nil, err
			}
			resp.Node.Endpoint = upstream.URL
			return resp, nil
		},
		recordAssignmentFunc: func(_ context.Context, _ *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, 5*time.Second, 1<<20)

	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(body))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("upstream saw %d attempts, want 2", len(seen))
	}
	for i, got := range seen {
		if got != body {
			t.Fatalf("attempt %d body = %q, want the original body", i+1, got)
		}
	}
}
