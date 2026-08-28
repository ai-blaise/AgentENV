package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"google.golang.org/grpc"
)

// Each gate is asserted in both directions. Off must remove the behaviour,
// which catches a flag that does nothing; on must produce it, which catches a
// flag wired to the wrong thing. See the same harness on the scheduler side
// for why this exists rather than being assumed.

// The binding cache reuses a lookup for a bounded window. Off, every request
// must re-resolve.
func TestOffSwitchBindingCache(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ttl         time.Duration
		wantLookups int32
	}{
		{name: "on reuses one lookup", ttl: time.Minute, wantLookups: 1},
		{name: "off re-resolves every request", ttl: -1, wantLookups: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var lookups atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer upstream.Close()

			client := &countingLookupClient{endpoint: upstream.URL, lookups: &lookups}
			server := newTestServer(t, client, 5*time.Second, 1<<20, func(o *ServerOptions) {
				o.BindingCacheTTL = tc.ttl
			})
			handler := authenticatedTestHandler(server)

			for i := 0; i < 3; i++ {
				request := httptest.NewRequest(http.MethodGet, "/", nil)
				request.Header.Set(headerSandboxID, "00000000-0000-7000-8000-0000000000aa")
				request.Header.Set(headerTargetPort, "8000")
				handler.ServeHTTP(httptest.NewRecorder(), request)
			}

			if got := lookups.Load(); got != tc.wantLookups {
				t.Fatalf("scheduler lookups = %d, want %d", got, tc.wantLookups)
			}
		})
	}
}

// The disown signal is what makes a stale binding fall out of the cache
// immediately. Without it — an older node that does not send the header — the
// cached binding must survive, which is the pre-change behaviour.
func TestOffSwitchDisownInvalidation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		disown      bool
		wantLookups int32
	}{
		// Three: the initial lookup, the cutover's own re-resolve, and the
		// second request finding the cache empty.
		{name: "a disowned sandbox re-resolves", disown: true, wantLookups: 3},
		{name: "a plain 404 keeps the cached binding", disown: false, wantLookups: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var lookups atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.disown {
					w.Header().Set(headerSandboxDisowned, "1")
				}
				http.Error(w, "not found", http.StatusNotFound)
			}))
			defer upstream.Close()

			client := &countingLookupClient{endpoint: upstream.URL, lookups: &lookups}
			server := newTestServer(t, client, 5*time.Second, 1<<20, func(o *ServerOptions) {
				o.BindingCacheTTL = time.Minute
			})
			handler := authenticatedTestHandler(server)

			for i := 0; i < 2; i++ {
				request := httptest.NewRequest(http.MethodGet, "/", nil)
				request.Header.Set(headerSandboxID, "00000000-0000-7000-8000-0000000000bb")
				request.Header.Set(headerTargetPort, "8000")
				handler.ServeHTTP(httptest.NewRecorder(), request)
			}

			if got := lookups.Load(); got != tc.wantLookups {
				t.Fatalf("scheduler lookups = %d, want %d", got, tc.wantLookups)
			}
		})
	}
}

// countingLookupClient answers every lookup with the same node and counts how
// often it was asked, which is how the cache's presence is observed.
type countingLookupClient struct {
	stubSchedulerClient
	endpoint string
	lookups  *atomic.Int32
}

func (c *countingLookupClient) LookupNode(
	_ context.Context,
	_ *schedulerv1.LookupNodeRequest,
	_ ...grpc.CallOption,
) (*schedulerv1.LookupNodeResponse, error) {
	c.lookups.Add(1)
	return &schedulerv1.LookupNodeResponse{
		Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: c.endpoint},
	}, nil
}
