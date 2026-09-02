package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	scheduler "agentenv/services/scheduler/internal"
	"agentenv/services/shared/config"
	"agentenv/services/shared/logging"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/client-go/rest"
)

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	modeFlag := flag.String("mode", "", "primary (default), replica, or query-only; replica and query-only require scheduler.redis_addr")
	queryOnly := flag.Bool("query-only", false, "deprecated alias for -mode=query-only")
	flag.Parse()

	mode, err := config.ResolveSchedulerMode(*modeFlag, *queryOnly, os.LookupEnv)
	if err != nil {
		log.Fatalf("resolve scheduler mode failed: %v", err)
	}

	cfg, err := config.LoadScheduler(*configPath, mode)
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}

	logger, err := logging.New(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		log.Fatalf("init logger failed: %v", err)
	}
	defer logger.Sync()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	scheduler.SetSchedulerModeMetric(string(mode))
	if mode == config.SchedulerModePrimary && strings.TrimSpace(cfg.Scheduler.RedisAddr) == "" {
		// Said once, at startup: bindings that live only in this process are
		// lost on every restart, and a second scheduler started against this
		// config would route from a map of its own.
		logger.Warn("scheduler is running without a shared binding store; bindings are lost on restart and cannot be replicated",
			zap.String("enable_with", "scheduler.redis_addr or SCHEDULER_REDIS_ADDR"),
		)
	}

	store, closeStore := createBindingStore(logger, cfg)
	defer closeStore()

	authToken, err := scheduler.ResolveAuthToken(cfg.Scheduler.AuthToken, cfg.Scheduler.AuthTokenFile)
	if err != nil {
		logger.Fatal("resolve scheduler auth token failed", zap.Error(err))
	}
	if !scheduler.AuthEnabled(authToken) {
		// Said once, at startup, and not again: an open listener is a
		// deployment choice the operator should see made, not a condition to
		// be reminded of on every RPC.
		logger.Warn("scheduler gRPC authentication is disabled; every RPC is accepted",
			zap.String("enable_with", "scheduler.auth_token, scheduler.auth_token_file or SCHEDULER_AUTH_TOKEN"),
		)
	}

	// Metrics outermost so a refused RPC is still timed and labelled
	// status=unauthenticated; the auth interceptor's own counter carries the
	// reason.
	g := grpc.NewServer(
		grpc.ChainUnaryInterceptor(scheduler.MetricsUnaryInterceptor(), scheduler.AuthUnaryInterceptor(authToken)),
		grpc.ChainStreamInterceptor(scheduler.AuthStreamInterceptor(authToken)),
	)
	hs := health.NewServer()
	gate := newReadinessGate(hs, logger)
	go scheduler.RunStoreProbe(sigCtx, store, gate, cfg.Scheduler.StoreProbeInterval, logger)

	streamEnabled := cfg.Scheduler.NodeStreamEnabledFor(mode)
	if mode.QueryOnly() {
		svc := scheduler.NewQueryOnlyService(logger, store)
		schedulerv1.RegisterSchedulerServer(g, svc)
		// A query-only scheduler holds no registry and discovers nothing, so
		// there is nothing for it to warm up: the store probe alone decides
		// whether it is ready.
		gate.MarkStarted()
		logger.Info("scheduler query-only service enabled", zap.String("redis_addr", cfg.Scheduler.RedisAddr))
	} else {
		base := scheduler.NewAtomicNodeRegistry(nil, cfg.Scheduler.ReportTTL)
		discoveryReady := make(chan struct{})
		switch strings.ToLower(strings.TrimSpace(cfg.Scheduler.Discovery.Mode)) {
		case "kubernetes":
			go runKubernetesDiscoveryWithRetry(sigCtx, logger, cfg.Scheduler.Discovery.Kubernetes, base, discoveryReady)
		default:
			nodes := make([]scheduler.Node, 0, len(cfg.Scheduler.Nodes))
			for _, n := range cfg.Scheduler.Nodes {
				nodes = append(nodes, scheduler.Node{ID: n.ID, Endpoint: n.Endpoint})
			}
			base.Set(nodes, nil)
			close(discoveryReady)
		}

		var registry scheduler.NodeRegistry = base
		if streamEnabled {
			stream, streamReady := startNodeStream(sigCtx, logger, cfg, base, mode, replicaID())
			if stream != nil {
				registry = stream
				defer func() {
					if err := stream.Close(); err != nil {
						logger.Warn("close scheduler node stream failed", zap.Error(err))
					}
				}()
			} else {
				streamEnabled = false
			}
			go waitForReadiness(sigCtx, logger, gate, discoveryReady, streamReady, cfg.Scheduler.NodeStreamWarmupTimeout)
		} else {
			go waitForReadiness(sigCtx, logger, gate, discoveryReady, nil, cfg.Scheduler.NodeStreamWarmupTimeout)
		}

		svc := scheduler.NewService(
			logger,
			registry,
			scheduler.NewStrategy(cfg.Scheduler.Strategy),
			store,
			// Without this the service validates the TTL ordering against the
			// hardcoded default rather than the TTL its store was actually
			// built with, so an operator raising binding_ttl changed the store
			// and nothing else.
			scheduler.WithBindingTTL(cfg.Scheduler.BindingTTL),
			scheduler.WithArtifactStoreCapacity(
				cfg.Scheduler.ArtifactStoreCapacity,
				cfg.Scheduler.ArtifactLookupNodeLimit,
			),
			scheduler.WithNodeResourceLimit(cfg.Scheduler.NodeResourceLimit),
			scheduler.WithReportTTL(cfg.Scheduler.ReportTTL),
			scheduler.WithHealthGate(cfg.Scheduler.HealthGateEnabled()),
			scheduler.WithReservations(cfg.Scheduler.ReservationsEnabled, cfg.Scheduler.MaxReservationDelta),
			// Mobility records follow the binding store: a deployment that put
			// bindings in Redis so a restart or a second replica could see
			// them needs paused-sandbox ownership there for the same reason.
			scheduler.WithMobilityStore(scheduler.MobilityStoreFor(store)),
		)
		go svc.RunObservedNodesMetrics(sigCtx, 15*time.Second)
		schedulerv1.RegisterSchedulerServer(g, svc)
	}

	grpc_health_v1.RegisterHealthServer(g, hs)

	lis, err := net.Listen("tcp", cfg.Scheduler.GRPCListenAddr)
	if err != nil {
		logger.Fatal("listen failed", zap.Error(err), zap.String("addr", cfg.Scheduler.GRPCListenAddr))
	}
	logger.Info("scheduler gRPC server listening",
		zap.String("addr", cfg.Scheduler.GRPCListenAddr),
		zap.String("strategy", cfg.Scheduler.Strategy),
		zap.String("binding_store", bindingStoreName(cfg)),
		zap.String("mobility_store", bindingStoreName(cfg)),
		zap.String("mode", string(mode)),
		zap.Bool("node_stream_enabled", streamEnabled),
		zap.Bool("auth_enabled", scheduler.AuthEnabled(authToken)),
		zap.Bool("reservations_enabled", cfg.Scheduler.ReservationsEnabled),
	)

	metricsServer := &http.Server{
		Addr:    cfg.Scheduler.MetricsListenAddr,
		Handler: promhttp.Handler(),
	}
	go func() {
		logger.Info("scheduler metrics server listening", zap.String("addr", metricsServer.Addr))
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("scheduler metrics serve failed", zap.Error(err))
		}
	}()

	serveErrCh := make(chan error, 1)
	go func() {
		err := g.Serve(lis)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErrCh <- err
			return
		}
		serveErrCh <- nil
	}()

	select {
	case err := <-serveErrCh:
		if err != nil {
			logger.Fatal("serve failed", zap.Error(err))
		}
		return
	case <-sigCtx.Done():
	}

	logger.Info("scheduler shutdown signal received")
	gate.Shutdown()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	gracefulStopDone := make(chan struct{})
	go func() {
		g.GracefulStop()
		close(gracefulStopDone)
	}()

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case <-gracefulStopDone:
		logger.Info("scheduler stopped gracefully")
	case <-timer.C:
		logger.Warn("scheduler graceful shutdown timed out; forcing stop")
		g.Stop()
		<-gracefulStopDone
	}

	metricsShutdownCtx, cancelMetricsShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelMetricsShutdown()
	if err := metricsServer.Shutdown(metricsShutdownCtx); err != nil {
		logger.Warn("scheduler metrics graceful shutdown failed", zap.Error(err))
	}

	if err := <-serveErrCh; err != nil {
		logger.Fatal("serve failed", zap.Error(err))
	}
}

// newReadinessGate splits what liveness reads from what readiness reads.
//
// The overall status answers "is this process alive", and is SERVING from the
// moment the listener is up. The named service answers "is this process fit for
// traffic", and starts closed: with replicas behind one round-robin client,
// reporting fitness at startup hands a replica its full share of placements
// while its registry is still empty, and a replica that has lost its binding
// store keeps taking them.
//
// The split matters because the shipped manifest probes liveness and readiness
// with the same health check. Gating the overall status would restart a
// scheduler for an unreachable Redis, turning an outage into a crash loop
// across the whole tier at once. A readiness probe that passes
// -service=scheduler.v1.Scheduler is what makes the gate remove a pod from the
// Service instead.
func newReadinessGate(health scheduler.HealthStatusSetter, logger *zap.Logger) *scheduler.ReadinessGate {
	health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	return scheduler.NewReadinessGate(health, logger, schedulerv1.Scheduler_ServiceDesc.ServiceName)
}

// replicaID names this process on the node-state bus, so a replica can skip its
// own echo. The pod name is stable across a restart, which keeps the metric
// series readable; the random suffix is only for deployments that do not set
// it, where self-echo skipping degrades to the ordinary staleness check.
func replicaID() string {
	if name := strings.TrimSpace(os.Getenv("POD_NAME")); name != "" {
		return name
	}
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "scheduler"
	}
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		return host
	}
	return host + "-" + hex.EncodeToString(suffix)
}

// startNodeStream builds the replicated registry and starts consuming the bus.
//
// A replica that cannot reach the bus at startup exits rather than running:
// without it, it hears from only the nodes whose connections happen to land on
// it and places all of its traffic there, which is strictly worse than the
// single scheduler it was scaled out from. Any other mode keeps running with a
// warning, because there the stream is an addition rather than the premise.
func startNodeStream(
	ctx context.Context,
	logger *zap.Logger,
	cfg config.Config,
	base *scheduler.AtomicNodeRegistry,
	mode config.SchedulerMode,
	replicaID string,
) (*scheduler.StreamFedNodeRegistry, <-chan struct{}) {
	bus, err := scheduler.NewRedisNodeStream(ctx, cfg.Scheduler.RedisAddr, scheduler.NodeStreamOptions{
		Logger:       logger,
		MaxLen:       int64(cfg.Scheduler.NodeStreamMaxLen),
		PublishQueue: cfg.Scheduler.NodeStreamPublishQueue,
		ReportTTL:    cfg.Scheduler.ReportTTL,
	})
	if err != nil {
		if mode == config.SchedulerModeReplica {
			logger.Fatal("connect scheduler node stream failed", zap.Error(err), zap.String("addr", cfg.Scheduler.RedisAddr))
		}
		logger.Error("connect scheduler node stream failed; this scheduler will see only the nodes that heartbeat to it",
			zap.Error(err),
			zap.String("addr", cfg.Scheduler.RedisAddr),
		)
		return nil, nil
	}

	registry := scheduler.NewStreamFedNodeRegistry(base, bus, replicaID, logger)
	ready, err := registry.Run(ctx)
	if err != nil {
		logger.Error("subscribe to scheduler node stream failed", zap.Error(err))
		if closeErr := bus.Close(); closeErr != nil {
			logger.Warn("close scheduler node stream failed", zap.Error(closeErr))
		}
		return nil, nil
	}
	return registry, ready
}

// waitForReadiness opens the readiness gate once this scheduler knows the fleet.
//
// The replay is bounded rather than waited out: NOT_SERVING-forever means a
// store blip during a rollout takes the whole tier down, while serving early
// leaves a partial registry that fails open exactly as a single scheduler does
// after a restart. The timeout is the operator's choice between those and the
// warm-up gauge says which one happened.
func waitForReadiness(
	ctx context.Context,
	logger *zap.Logger,
	gate *scheduler.ReadinessGate,
	discoveryReady <-chan struct{},
	streamReady <-chan struct{},
	warmupTimeout time.Duration,
) {
	select {
	case <-ctx.Done():
		return
	case <-discoveryReady:
	}

	if streamReady != nil {
		timer := time.NewTimer(warmupTimeout)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-streamReady:
		case <-timer.C:
			scheduler.MarkNodeStreamWarmupIncomplete()
			logger.Warn("scheduler node stream warm-up timed out; serving over a partial registry",
				zap.Duration("timeout", warmupTimeout),
			)
		}
	}
	gate.MarkStarted()
}

func createBindingStore(logger *zap.Logger, cfg config.Config) (scheduler.BindingStore, func()) {
	// Passing the three timings together is what lets the store refuse a
	// combination it cannot honour — a reconcile grace that outlives the
	// binding TTL, or one too short to cover a node's reporting interval.
	// Constructing with the TTL alone silently accepted both.
	opts := scheduler.BindingStoreOptions{
		BindingTTL:        cfg.Scheduler.BindingTTL,
		ReconcileGrace:    cfg.Scheduler.ReconcileGrace,
		HeartbeatInterval: cfg.Scheduler.HeartbeatInterval,
	}

	if strings.TrimSpace(cfg.Scheduler.RedisAddr) == "" {
		store, err := scheduler.NewInMemoryBindingStoreWithOptions(opts)
		if err != nil {
			logger.Fatal("create binding store failed", zap.Error(err))
		}
		return store, func() {}
	}

	store, err := scheduler.NewRedisBindingStoreWithOptions(cfg.Scheduler.RedisAddr, opts)
	if err != nil {
		logger.Fatal("create redis binding store failed", zap.Error(err), zap.String("addr", cfg.Scheduler.RedisAddr))
	}
	return store, func() {
		if err := store.Close(); err != nil {
			logger.Warn("close redis binding store failed", zap.Error(err))
		}
	}
}

func bindingStoreName(cfg config.Config) string {
	if strings.TrimSpace(cfg.Scheduler.RedisAddr) != "" {
		return "redis"
	}
	return "memory"
}

func runKubernetesDiscoveryWithRetry(
	ctx context.Context,
	logger *zap.Logger,
	cfg config.SchedulerDiscoveryKubernetesConfig,
	registry *scheduler.AtomicNodeRegistry,
	ready chan<- struct{},
) {
	var once sync.Once
	signalReady := func() {
		once.Do(func() { close(ready) })
	}
	defer signalReady()

	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 30 * time.Second
	)

	backoff := initialBackoff
	attempt := 0

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		attempt++
		discovery, err := scheduler.NewKubernetesDiscovery(logger, cfg, registry)
		if err != nil {
			if errors.Is(err, rest.ErrNotInCluster) {
				logger.Error("kubernetes discovery initialization failed with non-retryable error; stopping discovery loop",
					zap.Error(err),
					zap.Int("attempt", attempt),
				)
				return
			}

			logger.Warn("kubernetes discovery initialization failed; retrying",
				zap.Error(err),
				zap.Int("attempt", attempt),
				zap.Duration("retry_in", backoff),
			)
			if !sleepWithContext(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		go func() {
			select {
			case <-ctx.Done():
			case <-discovery.Ready():
				signalReady()
			}
		}()

		err = discovery.Run(ctx)
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}

		logger.Warn("kubernetes discovery stopped unexpectedly; retrying",
			zap.Error(err),
			zap.Int("attempt", attempt),
			zap.Duration("retry_in", backoff),
		)
		if !sleepWithContext(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(current time.Duration, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}
