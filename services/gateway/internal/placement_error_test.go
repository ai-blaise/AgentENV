package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Retry-After is the signal a client acts on without reading anything else,
// so every code the scheduler can return is pinned for both the status and
// whether it says when to come back.
func TestWriteSchedulerErrorMapsEachCode(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)
	for _, tc := range []struct {
		name           string
		err            error
		wantStatus     int
		wantRetryAfter bool
	}{
		{name: "invalid argument", err: status.Error(codes.InvalidArgument, "bad hint"), wantStatus: http.StatusBadRequest},
		{name: "not found", err: status.Error(codes.NotFound, "no binding"), wantStatus: http.StatusNotFound},
		{name: "resource exhausted carries Retry-After", err: status.Error(codes.ResourceExhausted, "all candidates refused"), wantStatus: http.StatusServiceUnavailable, wantRetryAfter: true},
		{name: "unavailable carries none", err: status.Error(codes.Unavailable, "no nodes available"), wantStatus: http.StatusServiceUnavailable},
		{name: "internal is a bad gateway", err: status.Error(codes.Internal, "boom"), wantStatus: http.StatusBadGateway},
		{name: "a non-status error is a bad gateway", err: errors.New("dial failed"), wantStatus: http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			server.writeSchedulerError(rec, tc.err)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if got := rec.Header().Get("Retry-After") != ""; got != tc.wantRetryAfter {
				t.Fatalf("Retry-After present = %t, want %t", got, tc.wantRetryAfter)
			}
			if got := rec.Header().Get(headerRefusalReason); got != "" {
				t.Fatalf("a non-create scheduler error carried refusal %q; the reason vocabulary is for creates", got)
			}
		})
	}
}

// refusalCounts reads agentenv_gateway_create_refusals_total by reason.
func refusalCounts(t *testing.T) map[string]float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	counts := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "agentenv_gateway_create_refusals_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == "reason" {
					counts[pair.GetValue()] += metric.GetCounter().GetValue()
				}
			}
		}
	}
	return counts
}

// The scheduler has two ways of saying "no node", and they call for opposite
// client behaviour. Nodes that all declined is capacity, which frees up, so
// the refusal says when to retry. No node to ask — none discovered, or no
// scheduler reachable — is not cured by waiting, so it does not. Both speak
// the one refusal header the node and the gateway already share.
func TestScheduledCreateMapsPlacementFailuresOntoTheRefusalHeader(t *testing.T) {
	for _, tc := range []struct {
		name           string
		err            error
		wantStatus     int
		wantReason     string
		wantRetryAfter bool
	}{
		{
			name:           "resource exhausted is fleet_exhausted with Retry-After",
			err:            status.Error(codes.ResourceExhausted, "all candidate nodes rejected the request"),
			wantStatus:     http.StatusServiceUnavailable,
			wantReason:     refusalFleetExhausted,
			wantRetryAfter: true,
		},
		{
			name:       "unavailable is no_nodes without Retry-After",
			err:        status.Error(codes.Unavailable, "no nodes available"),
			wantStatus: http.StatusServiceUnavailable,
			wantReason: refusalNoNodes,
		},
		{
			name:       "anything else is the plain scheduler mapping",
			err:        status.Error(codes.Internal, "strategy panicked"),
			wantStatus: http.StatusBadGateway,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := refusalCounts(t)
			var scheduleCalls atomic.Int32
			server := newTestServer(t, stubSchedulerClient{
				scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
					scheduleCalls.Add(1)
					return nil, tc.err
				},
			}, time.Second, 1<<20)

			req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
			req.Header.Set(headerAPIKey, testAPIKey)
			rec := httptest.NewRecorder()
			server.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if got := rec.Header().Get(headerRefusalReason); got != tc.wantReason {
				t.Fatalf("%s = %q, want %q", headerRefusalReason, got, tc.wantReason)
			}
			if got := rec.Header().Get("Retry-After") != ""; got != tc.wantRetryAfter {
				t.Fatalf("Retry-After present = %t, want %t", got, tc.wantRetryAfter)
			}
			if got := scheduleCalls.Load(); got != 1 {
				t.Fatalf("Schedule called %d times, want 1: a placement failure is not retried", got)
			}
			after := refusalCounts(t)
			if tc.wantReason != "" {
				if got := after[tc.wantReason] - before[tc.wantReason]; got != 1 {
					t.Fatalf("refusals{reason=%s} counted %v, want 1", tc.wantReason, got)
				}
			}
		})
	}
}

// A management request that the scheduler cannot place is answered with the
// same Retry-After rule, but without a create refusal reason: it was not a
// create, and it is not counted as one.
func TestManagementRequestSchedulerFailureCarriesNoRefusalReason(t *testing.T) {
	before := refusalCounts(t)
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "all candidates refused")
		},
	}, time.Second, 1<<20)

	req := httptest.NewRequest(http.MethodGet, "/templates", nil)
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("ResourceExhausted must carry Retry-After on every path")
	}
	if got := rec.Header().Get(headerRefusalReason); got != "" {
		t.Fatalf("a management request carried create refusal %q", got)
	}
	after := refusalCounts(t)
	for reason := range after {
		if after[reason] != before[reason] {
			t.Fatalf("refusals{reason=%s} moved on a management request", reason)
		}
	}
}

// Fork is routed by the source sandbox and physically cannot run anywhere
// else: the children are made from a VM that lives on exactly one node. A
// capacity refusal from that node is therefore terminal, and asking the
// scheduler for another node would offer the fork to a node that does not
// have the parent.
func TestForkIsNeverRescheduled(t *testing.T) {
	refusal := loadNodeAdmission503(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		refusal.replay(w)
	}))
	defer upstream.Close()

	var scheduleCalls atomic.Int32
	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "sbx-parent" {
				return nil, status.Error(codes.NotFound, "not found")
			}
			return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}}, nil
		},
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			scheduleCalls.Add(1)
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-b", Endpoint: upstream.URL}}, nil
		},
	}, 5*time.Second, 1<<20)

	req := httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-parent/fork", strings.NewReader(`{"count":2}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want the parent node's 503 surfaced", rec.Code)
	}
	if got := rec.Header().Get(headerRefusalReason); got != refusalNodeAtCapacity {
		t.Fatalf("%s = %q, want the node's own %q: a fork refusal is terminal", headerRefusalReason, got, refusalNodeAtCapacity)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("a fork refused for capacity must still say when to retry")
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("parent node asked %d times, want exactly once", got)
	}
	if got := scheduleCalls.Load(); got != 0 {
		t.Fatalf("Schedule called %d times, want never: fork cannot be placed elsewhere", got)
	}
}

// upstreamProxyCounts reads the sample count of
// agentenv_gateway_upstream_proxy_duration_seconds for one route, by status.
func upstreamProxyCounts(t *testing.T, route string) map[string]uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	counts := map[string]uint64{}
	for _, family := range families {
		if family.GetName() != "agentenv_gateway_upstream_proxy_duration_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["route"] != route {
				continue
			}
			counts[labels["status"]] += metric.GetHistogram().GetSampleCount()
		}
	}
	return counts
}

// An attempt a node refused is a 5xx to the upstream duration metric and the
// attempt that accepted is a 2xx, all the way through the proxy: the refused
// attempt was written into a buffer the client never saw, and a metric that
// read the client's writer would report it as a success.
func TestARefusedPlacementAttemptIsCountedAsA5xxUpstream(t *testing.T) {
	refusal := loadNodeAdmission503(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			refusal.replay(w)
			return
		}
		w.Header().Set(headerSandboxID, "sbx-1")
		w.WriteHeader(http.StatusCreated)
	}))
	defer upstream.Close()

	before := upstreamProxyCounts(t, "/sandboxes")

	recorder := &scheduleRecorder{nodes: []string{"refusing-node", "accepting-node"}}
	server := newRescheduleTestServer(t, recorder, upstream)
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}

	after := upstreamProxyCounts(t, "/sandboxes")
	if got := after["5xx"] - before["5xx"]; got != 1 {
		t.Fatalf("upstream 5xx observations = %d, want the refused attempt counted once", got)
	}
	if got := after["2xx"] - before["2xx"]; got != 1 {
		t.Fatalf("upstream 2xx observations = %d, want the accepted attempt counted once", got)
	}
	if got := after["other"] - before["other"]; got != 0 {
		t.Fatalf("upstream 'other' observations = %d, want none: a buffered attempt must report its status", got)
	}
}

// The binding a create just produced is in the gateway's cache before the
// client's next request arrives, for the single-sandbox header path and the
// fork body path alike.
func TestACreatedSandboxIsRoutedFromTheCacheWithoutALookup(t *testing.T) {
	var mu sync.Mutex
	var upstreamPaths []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		upstreamPaths = append(upstreamPaths, r.URL.Path)
		mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sandboxes":
			w.Header().Set(headerSandboxID, "sbx-created")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"sandboxID":"sbx-created"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/sandboxes/sbx-created/fork":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`[{"sandbox":{"sandboxID":"child-1"}},{"sandbox":{"sandboxID":"child-2"}}]`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer upstream.Close()

	node := &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}
	var lookups atomic.Int32
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return &schedulerv1.ScheduleResponse{Node: node}, nil
		},
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			lookups.Add(1)
			return nil, status.Error(codes.NotFound, "the scheduler has not seen it yet")
		},
		recordAssignmentFunc: func(context.Context, *schedulerv1.RecordAssignmentRequest, ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, 5*time.Second, 1<<20)
	handler := authenticatedTestHandler(server)

	create := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, create)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", rec.Code)
	}

	// The fork is routed by the parent, which the create just cached, and
	// records its children through the batch path.
	fork := httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-created/fork", strings.NewReader(`{"count":2}`))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, fork)
	if rec.Code != http.StatusCreated {
		t.Fatalf("fork status = %d, want 201; body=%q", rec.Code, rec.Body.String())
	}

	for _, sandboxID := range []string{"sbx-created", "child-1", "child-2"} {
		use := httptest.NewRequest(http.MethodGet, "/", nil)
		use.Header.Set(headerSandboxID, sandboxID)
		use.Header.Set(headerTargetPort, "8000")
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, use)
		if rec.Code != http.StatusOK {
			t.Fatalf("request to %s status = %d, want 200 routed to the creating node", sandboxID, rec.Code)
		}
	}
	if got := lookups.Load(); got != 0 {
		t.Fatalf("LookupNode called %d times, want 0: every binding was recorded into the cache by the create that made it", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(upstreamPaths) != 5 {
		t.Fatalf("upstream saw %v, want the create, the fork and three data-plane requests", upstreamPaths)
	}
}
