package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A binding miss on the cheap path is retried against the primary.
//
// The query-only replica is a cached binding read and cannot answer for a
// sandbox with no binding -- it has no node registry and no strategy. A paused
// sandbox whose binding lapsed is exactly that, and returning its NotFound
// straight through made it a 404 for a sandbox the client can still list.
func TestLookupFallsBackToThePrimaryOnNotFound(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	var primaryCalls atomic.Int64
	primary := stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			primaryCalls.Add(1)
			if req.GetSandboxId() != "sbx-paused" {
				return nil, fmt.Errorf("fallback asked for %q", req.GetSandboxId())
			}
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-1", Endpoint: upstream.URL},
			}, nil
		},
	}
	queryOnly := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return nil, status.Error(codes.NotFound, "sandbox assignment not found")
		},
	}
	server := newTestServer(t, primary, time.Second, 1024, withQueryOnlyScheduler(queryOnly))

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/health", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Header.Set(headerSandboxID, "sbx-paused")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d: the fallback did not route the request",
			resp.StatusCode, http.StatusNoContent)
	}
	if primaryCalls.Load() != 1 {
		t.Fatalf("primary lookups = %d, want exactly 1", primaryCalls.Load())
	}
}

// An error that is not a miss is not retried.
//
// Unavailable means the cheap path failed. Asking a second scheduler the same
// question inside the same request budget spends the client's deadline to
// arrive at the same answer.
func TestLookupDoesNotFallBackOnUnavailable(t *testing.T) {
	var primaryCalls atomic.Int64
	primary := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			primaryCalls.Add(1)
			return nil, fmt.Errorf("primary should not be consulted")
		},
	}
	queryOnly := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return nil, status.Error(codes.Unavailable, "binding store unavailable")
		},
	}
	server := newTestServer(t, primary, time.Second, 1024, withQueryOnlyScheduler(queryOnly))

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/health", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Header.Set(headerSandboxID, "sbx-any")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if primaryCalls.Load() != 0 {
		t.Fatalf("primary was consulted %d times on a non-miss error", primaryCalls.Load())
	}
}
