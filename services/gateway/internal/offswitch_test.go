package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"google.golang.org/grpc"
)

// Each gate is asserted in both directions. Off must remove the behaviour,
// which catches a flag that does nothing; on must produce it, which catches a
// flag wired to the wrong thing. See the same harness on the scheduler side
// for why this exists rather than being assumed.

// The binding cache reuses a lookup for a bounded window. Off, every request
// must re-resolve. gateway.binding_cache_size is the switch — zero or
// negative — and a negative gateway.binding_cache_ttl keeps meaning the same,
// so a deployment that turned the cache off before this key existed stays off.
func TestOffSwitchBindingCache(t *testing.T) {
	for _, tc := range []struct {
		name        string
		size        int
		ttl         time.Duration
		wantLookups int32
	}{
		{name: "on reuses one lookup", size: config.DefaultGatewayBindingCacheSize, ttl: time.Minute, wantLookups: 1},
		{name: "off by size zero re-resolves every request", size: 0, ttl: time.Minute, wantLookups: 3},
		{name: "off by negative size re-resolves every request", size: -1, ttl: time.Minute, wantLookups: 3},
		{name: "off by negative ttl re-resolves every request", size: config.DefaultGatewayBindingCacheSize, ttl: -1, wantLookups: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var lookups atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer upstream.Close()

			client := &countingLookupClient{endpoint: upstream.URL, lookups: &lookups}
			server := newTestServer(t, client, 5*time.Second, 1<<20, func(o *ServerOptions) {
				o.BindingCache = BindingCacheOptions{Size: tc.size, TTL: tc.ttl}
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
				o.BindingCache.TTL = time.Minute
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

// The scheduler credential is off when no token is configured, which is how a
// gateway keeps talking to a scheduler that does not enforce one yet. On, the
// metadata is on the wire of every call.
func TestOffSwitchSchedulerAuthToken(t *testing.T) {
	for _, tc := range []struct {
		name      string
		token     string
		wantValue []string
	}{
		{name: "on presents the bearer token", token: "shared-secret", wantValue: []string{"Bearer shared-secret"}},
		{name: "off sends no credential", token: "", wantValue: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, seen := startRecordingScheduler(t)
			client := dialScheduler(t, addr, tc.token)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := client.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"}); err != nil {
				t.Fatalf("LookupNode: %v", err)
			}

			got := receiveMetadata(t, seen).Get("authorization")
			if len(got) != len(tc.wantValue) {
				t.Fatalf("authorization = %v, want %v", got, tc.wantValue)
			}
			for i := range got {
				if got[i] != tc.wantValue[i] {
					t.Fatalf("authorization = %v, want %v", got, tc.wantValue)
				}
			}
		})
	}
}

// Create rescheduling is what turns a node's capacity refusal into a placement
// elsewhere. Off — a negative gateway.max_schedule_retries — the first node's
// refusal is the client's answer, exactly as before the loop existed; on, the
// shipped default, the create is offered to a second node and succeeds there.
func TestOffSwitchCreateRescheduling(t *testing.T) {
	for _, tc := range []struct {
		name         string
		retries      int
		wantStatus   int
		wantReason   string
		wantAttempts int32
	}{
		{name: "on offers the create to a second node", retries: 0, wantStatus: http.StatusCreated, wantAttempts: 2},
		{name: "off passes the first node's refusal through", retries: -1, wantStatus: http.StatusServiceUnavailable, wantReason: refusalNodeAtCapacity, wantAttempts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refusal := loadNodeAdmission503(t)
			var attempts atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if attempts.Add(1) == 1 {
					refusal.replay(w)
					return
				}
				w.Header().Set(headerSandboxID, "sbx-1")
				w.WriteHeader(http.StatusCreated)
			}))
			defer upstream.Close()

			recorder := &scheduleRecorder{nodes: []string{"refusing-node", "accepting-node"}}
			server := newRescheduleTestServer(t, recorder, upstream, func(o *ServerOptions) {
				o.MaxScheduleRetries = tc.retries
			})

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
			if got := attempts.Load(); got != tc.wantAttempts {
				t.Fatalf("nodes asked = %d, want %d", got, tc.wantAttempts)
			}
		})
	}
}
