package main

import (
	"sync"
	"testing"

	"google.golang.org/grpc/health/grpc_health_v1"
)

type recordingHealth struct {
	mu       sync.Mutex
	statuses map[string]grpc_health_v1.HealthCheckResponse_ServingStatus
}

func (h *recordingHealth) SetServingStatus(service string, status grpc_health_v1.HealthCheckResponse_ServingStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.statuses == nil {
		h.statuses = map[string]grpc_health_v1.HealthCheckResponse_ServingStatus{}
	}
	h.statuses[service] = status
}

func (h *recordingHealth) status(service string) grpc_health_v1.HealthCheckResponse_ServingStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.statuses[service]
}

// The shipped manifest probes liveness and readiness with the same gRPC health
// check. A scheduler that reported itself NOT_SERVING on the overall status
// while its store was unreachable would be restarted for it, so a Redis outage
// would crash-loop every replica at once instead of taking them out of
// rotation. Readiness therefore owns the named service and nothing else.
func TestReadinessNeverGatesTheStatusLivenessReads(t *testing.T) {
	health := &recordingHealth{}
	gate := newReadinessGate(health, nil)

	if got := health.status(""); got != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("overall status = %v at startup, want SERVING for the liveness probe", got)
	}
	if got := health.status("scheduler.v1.Scheduler"); got != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("scheduler service = %v at startup, want NOT_SERVING until the process is fit", got)
	}

	gate.MarkStarted()
	gate.SetStoreReachable(true)
	gate.SetStoreReachable(false)

	if got := health.status(""); got != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("overall status = %v after the store went away, want the liveness probe left alone", got)
	}
	if got := health.status("scheduler.v1.Scheduler"); got != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("scheduler service = %v after the store went away, want NOT_SERVING", got)
	}
}
