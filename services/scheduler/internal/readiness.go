package scheduler

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// storeProbeSandboxID is the key the store probe looks up. It names no sandbox,
// so the answer is always "no binding" and the probe measures the round trip
// rather than the data. An empty id would not do: the stores answer that
// without touching Redis.
const storeProbeSandboxID = "agentenv-scheduler-store-probe"

// storeProbeFailureThreshold is how many consecutive failed probes take a
// scheduler out of service. One failure is a timeout under load; three in a row
// at the probe interval is a store this process cannot reach.
const storeProbeFailureThreshold = 3

// HealthStatusSetter is the part of grpc's health server that readiness needs.
type HealthStatusSetter interface {
	SetServingStatus(service string, status grpc_health_v1.HealthCheckResponse_ServingStatus)
}

// ReadinessGate decides when a scheduler is fit to take traffic.
//
// With one scheduler this hardly matters: nothing else can take the traffic, so
// reporting SERVING before the registry is populated only means the first few
// placements are made over a thin view. With round-robin load balancing across
// replicas it is a defect in two directions. A replica that reports SERVING at
// startup is handed its full share of placements while its registry is empty,
// and it fails open onto a fleet it has no capacity numbers for. A replica that
// cannot reach the binding store keeps its share forever, because gRPC's
// round-robin balancer only drops a subchannel that fails to connect — one that
// answers every call with an application error stays in the picker.
//
// So readiness is a conjunction: the process has finished starting, and its
// store answered recently.
type ReadinessGate struct {
	health   HealthStatusSetter
	services []string
	logger   *zap.Logger

	mu             sync.Mutex
	started        bool
	storeReachable bool
	serving        bool
}

// NewReadinessGate reports NOT_SERVING until told otherwise.
func NewReadinessGate(health HealthStatusSetter, logger *zap.Logger, services ...string) *ReadinessGate {
	if logger == nil {
		logger = zap.NewNop()
	}
	gate := &ReadinessGate{health: health, services: services, logger: logger}
	gate.publish(grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	return gate
}

// MarkStarted says discovery has synced and any replay has finished.
func (g *ReadinessGate) MarkStarted() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.started = true
	g.reconcileLocked()
}

// SetStoreReachable records the binding store's answer to the latest probe.
func (g *ReadinessGate) SetStoreReachable(reachable bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.storeReachable = reachable
	g.reconcileLocked()
}

// Serving reports the status currently published.
func (g *ReadinessGate) Serving() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.serving
}

// Shutdown reports NOT_SERVING for good.
func (g *ReadinessGate) Shutdown() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.started = false
	g.serving = false
	g.publish(grpc_health_v1.HealthCheckResponse_NOT_SERVING)
}

func (g *ReadinessGate) reconcileLocked() {
	serving := g.started && g.storeReachable
	if serving == g.serving {
		return
	}
	g.serving = serving
	if serving {
		g.logger.Info("scheduler is serving")
		g.publish(grpc_health_v1.HealthCheckResponse_SERVING)
		return
	}
	g.logger.Warn("scheduler stopped serving",
		zap.Bool("started", g.started),
		zap.Bool("store_reachable", g.storeReachable),
	)
	g.publish(grpc_health_v1.HealthCheckResponse_NOT_SERVING)
}

func (g *ReadinessGate) publish(status grpc_health_v1.HealthCheckResponse_ServingStatus) {
	if g.health == nil {
		return
	}
	for _, service := range g.services {
		g.health.SetServingStatus(service, status)
	}
}

// storeProbe asks the binding store whether it is still there, and reports the
// answer to readiness once it has seen enough of them to be sure.
type storeProbe struct {
	store    BindingStore
	gate     *ReadinessGate
	logger   *zap.Logger
	failures int
}

// step runs one probe. A single failure is a timeout under load and changes
// nothing; consecutive ones are a store this process cannot reach.
func (p *storeProbe) step() {
	_, _, err := p.store.Get(storeProbeSandboxID, time.Now())
	if err == nil {
		if p.failures >= storeProbeFailureThreshold {
			p.logger.Info("scheduler binding store is reachable again")
		}
		p.failures = 0
		schedulerStoreReachable.Set(1)
		p.gate.SetStoreReachable(true)
		return
	}

	p.failures++
	if p.failures == storeProbeFailureThreshold {
		p.logger.Error("scheduler binding store is unreachable; withdrawing from load balancing",
			zap.Int("consecutive_failures", p.failures),
			zap.Error(err),
		)
	}
	if p.failures >= storeProbeFailureThreshold {
		schedulerStoreReachable.Set(0)
		p.gate.SetStoreReachable(false)
	}
}

// RunStoreProbe keeps the gate's view of the binding store current until ctx is
// done. It probes immediately, so a scheduler whose store is up is ready
// without waiting out an interval.
func RunStoreProbe(ctx context.Context, store BindingStore, gate *ReadinessGate, interval time.Duration, logger *zap.Logger) {
	if store == nil || gate == nil {
		return
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	probe := &storeProbe{store: store, gate: gate, logger: logger}
	probe.step()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probe.step()
		}
	}
}

// SetSchedulerModeMetric publishes which mode this process runs in.
func SetSchedulerModeMetric(mode string) {
	schedulerMode.WithLabelValues(mode).Set(1)
}
