package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// recordingHealth is the slice of grpc's health server the gate drives.
type recordingHealth struct {
	mu       sync.Mutex
	statuses map[string]grpc_health_v1.HealthCheckResponse_ServingStatus
}

func newRecordingHealth() *recordingHealth {
	return &recordingHealth{statuses: map[string]grpc_health_v1.HealthCheckResponse_ServingStatus{}}
}

func (h *recordingHealth) SetServingStatus(service string, status grpc_health_v1.HealthCheckResponse_ServingStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.statuses[service] = status
}

func (h *recordingHealth) status(service string) grpc_health_v1.HealthCheckResponse_ServingStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.statuses[service]
}

func (h *recordingHealth) touched(service string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.statuses[service]
	return ok
}

// A replica that reports SERVING before it knows the fleet is handed its full
// share of placements over an empty registry, which fails open and places
// blind. Readiness therefore starts closed and needs both halves.
func TestReadinessGateNeedsAStartedProcessAndAReachableStore(t *testing.T) {
	health := newRecordingHealth()
	gate := NewReadinessGate(health, nil, "scheduler.v1.Scheduler")

	if got := health.status("scheduler.v1.Scheduler"); got != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("the gated service started as %v, want NOT_SERVING", got)
	}

	gate.MarkStarted()
	if gate.Serving() {
		t.Fatal("a started scheduler with no store answer is serving")
	}

	gate.SetStoreReachable(true)
	if !gate.Serving() {
		t.Fatal("a started scheduler with a reachable store is not serving")
	}
	if got := health.status("scheduler.v1.Scheduler"); got != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("the gated service is %v after readiness opened, want SERVING", got)
	}

	gate.SetStoreReachable(false)
	if health.status("scheduler.v1.Scheduler") != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatal("a scheduler that lost its store kept reporting itself fit for traffic")
	}

	// The overall status is what liveness probes read, and the gate must never
	// touch it: restarting a scheduler because Redis is unreachable turns an
	// outage into a crash loop across every replica at once.
	if health.touched("") {
		t.Fatal("readiness moved the overall health status, which liveness probes read")
	}
}

// flakyStore answers or fails on command, so a probe can be stepped through a
// partition and back without waiting on a clock.
type flakyStore struct {
	failingBindingStore
	mu      sync.Mutex
	healthy bool
	asked   string
}

func (s *flakyStore) setHealthy(healthy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthy = healthy
}

func (s *flakyStore) lastSandboxID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.asked
}

func (s *flakyStore) Get(sandboxID string, now time.Time) (Node, bool, error) {
	s.mu.Lock()
	s.asked = sandboxID
	healthy := s.healthy
	s.mu.Unlock()
	if !healthy {
		return s.failingBindingStore.Get(sandboxID, now)
	}
	return Node{}, false, nil
}

// gRPC's round-robin balancer drops a subchannel that cannot connect, not one
// that answers with an error, so a replica whose store is unreachable keeps
// taking its share of routing lookups and failing all of them. The probe is
// what takes it out of the picker.
//
// A single failure must not: that is a timeout under load, and withdrawing on
// it would make every replica flap under exactly the load that needs them.
func TestStoreProbeWithdrawsOnlyAfterRepeatedFailures(t *testing.T) {
	gate := NewReadinessGate(newRecordingHealth(), nil, "")
	gate.MarkStarted()
	store := &flakyStore{healthy: true}
	probe := &storeProbe{store: store, gate: gate, logger: zap.NewNop()}

	probe.step()
	if !gate.Serving() {
		t.Fatal("a scheduler whose store answered is not serving")
	}

	// Three is written out rather than read from storeProbeFailureThreshold:
	// driving the loop from the same constant production reads makes the
	// assertion vacuous, and it would stay green if the constant were retuned to
	// one — which is the change that makes every replica flap. Three is the
	// contract documented in services/README.md.
	const wantFailures = 3

	store.setHealthy(false)
	for i := 1; i < wantFailures; i++ {
		probe.step()
		if !gate.Serving() {
			t.Fatalf("the scheduler withdrew after %d failed probes, before the threshold of %d",
				i, wantFailures)
		}
	}

	probe.step()
	if gate.Serving() {
		t.Fatalf("the scheduler kept serving after %d consecutive failed probes", wantFailures)
	}

	store.setHealthy(true)
	probe.step()
	if !gate.Serving() {
		t.Fatal("the scheduler never came back after its store answered again")
	}
}

// The probe has to reach the store to measure anything. Both stores answer an
// empty sandbox id from the argument alone, without a round trip, so a probe
// that asked for one would report a partitioned Redis as healthy forever.
func TestStoreProbeAsksForAnIDTheStoreRoundTrips(t *testing.T) {
	gate := NewReadinessGate(newRecordingHealth(), nil, "")
	store := &flakyStore{healthy: true}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go RunStoreProbe(ctx, store, gate, time.Millisecond, nil)

	deadline := time.Now().Add(5 * time.Second)
	for store.lastSandboxID() == "" {
		if time.Now().After(deadline) {
			t.Fatal("the store probe never asked the store for a sandbox it would look up")
		}
		time.Sleep(time.Millisecond)
	}
}
