package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	gateway "agentenv/services/gateway/internal"
	"agentenv/services/shared/config"
	"agentenv/services/shared/logging"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

const (
	apiKeyEnv         = "AENV_API_KEY"
	defaultAPIKeyPath = "/run/secrets/api-key"
	maxAPIKeyLen      = 256
	maxAPIKeyFileLen  = maxAPIKeyLen + 2
)

// schedulerLoadBalancingConfig spreads RPCs over every scheduler the resolver
// names.
//
// Without it grpc.NewClient balances with pick_first, which uses one address
// and keeps one connection: every gateway pins itself to whichever scheduler it
// resolved first, so scaling the tier out moves no traffic and rolling it
// blacks routing out until the client reconnects. round_robin also drops a
// subchannel that fails to connect, which is what makes killing a replica
// invisible to clients.
const schedulerLoadBalancingConfig = `{"loadBalancingConfig":[{"round_robin":{}}]}`

func newSchedulerConn(addr string, authToken string) (*grpc.ClientConn, error) {
	target, resolverOptions, err := schedulerDialTarget(addr)
	if err != nil {
		return nil, err
	}
	options := append(
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(schedulerLoadBalancingConfig),
		},
		gateway.SchedulerDialOptions(authToken)...,
	)
	return grpc.NewClient(target, append(options, resolverOptions...)...)
}

// schedulerDialTarget turns a configured address into something to dial.
//
// A single address dials exactly as before — in Kubernetes that is a headless
// service name, and its DNS record is what makes round-robin span the replicas.
// A comma-separated list is resolved from the list itself, which mirrors
// scheduler.redis_addr and is the only form available where there is no
// cluster DNS to ask.
func schedulerDialTarget(addr string) (string, []grpc.DialOption, error) {
	addresses := make([]resolver.Address, 0, 1)
	for _, part := range strings.Split(addr, ",") {
		if part = strings.TrimSpace(part); part != "" {
			addresses = append(addresses, resolver.Address{Addr: part})
		}
	}
	switch len(addresses) {
	case 0:
		return "", nil, fmt.Errorf("scheduler address %q names no host", addr)
	case 1:
		return addresses[0].Addr, nil, nil
	}

	// The builder is passed to this connection alone rather than registered
	// globally, so the two scheduler connections cannot collide on a scheme.
	builder := manual.NewBuilderWithScheme("agentenv-scheduler")
	builder.InitialState(resolver.State{Addresses: addresses})
	return builder.Scheme() + ":///scheduler", []grpc.DialOption{grpc.WithResolvers(builder)}, nil
}

// serverOptionsFromConfig is the one place a gateway config key becomes a
// server option. A key that parses, documents, and is then never threaded is a
// setting that silently does nothing; keeping the mapping in one testable
// function is what lets a test say every key arrives.
func serverOptionsFromConfig(cfg config.GatewayConfig, apiKey string, queryOnly schedulerv1.SchedulerClient) gateway.ServerOptions {
	return gateway.ServerOptions{
		RequestTimeout:           cfg.RequestTimeout,
		MaxResponseSize:          cfg.ForwardResponseSize,
		APIKey:                   apiKey,
		DebugMode:                cfg.DebugMode,
		SandboxProxyDomains:      cfg.SandboxProxyDomains,
		QueryOnlySchedulerClient: queryOnly,
		MaxIdleConnsPerHost:      cfg.MaxIdleConnsPerHost,
		BindingCache: gateway.BindingCacheOptions{
			Size:        cfg.BindingCacheSize,
			TTL:         cfg.BindingCacheTTL,
			NegativeTTL: cfg.BindingCacheNegativeTTL,
		},
		MaxInFlightCreates: cfg.MaxInFlightCreates,
		MaxScheduleRetries: cfg.MaxScheduleRetries,
	}
}

func loadAPIKey() (string, error) {
	return loadAPIKeyFrom(os.LookupEnv, defaultAPIKeyPath)
}

func loadAPIKeyFrom(lookupEnv func(string) (string, bool), secretPath string) (string, error) {
	value, source := "", apiKeyEnv
	if explicit, present := lookupEnv(apiKeyEnv); present {
		value = explicit
	} else {
		file, err := openSecretFile(secretPath)
		if err != nil {
			if os.IsNotExist(err) {
				return "", fmt.Errorf("%s must be set or %s must exist", apiKeyEnv, secretPath)
			}
			return "", fmt.Errorf("read secret %s: %w", secretPath, err)
		}
		defer file.Close()
		contents, err := io.ReadAll(io.LimitReader(file, maxAPIKeyFileLen+1))
		if err != nil {
			return "", fmt.Errorf("read secret %s: %w", secretPath, err)
		}
		value = strings.TrimSuffix(strings.TrimSuffix(string(contents), "\n"), "\r")
		source = secretPath
	}
	return validateAPIKey(value, source)
}

func openSecretFile(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("open returned an invalid file descriptor")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("must be a regular file")
	}
	return file, nil
}

func validateAPIKey(value, source string) (string, error) {
	if len(value) < 32 || len(value) > maxAPIKeyLen {
		return "", fmt.Errorf("API key from %s must contain between 32 and %d URL-safe characters", source, maxAPIKeyLen)
	}
	for _, char := range []byte(value) {
		if (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '.' || char == '_' || char == '~' || char == '-' {
			continue
		}
		return "", fmt.Errorf("API key from %s must contain between 32 and %d URL-safe characters", source, maxAPIKeyLen)
	}
	return value, nil
}

// How long a client may take to send its request headers.
//
// Bounds Slowloris without touching the body, which matters because this
// proxy carries uploads and long-lived streams that legitimately take minutes.
const gatewayReadHeaderTimeout = 20 * time.Second

// How long an idle keep-alive connection is held open.
const gatewayIdleTimeout = 120 * time.Second

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	flag.Parse()

	cfg, err := config.Load(*configPath, "gateway")
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}
	apiKey, err := loadAPIKey()
	if err != nil {
		log.Fatalf("load API key failed: %v", err)
	}
	logger, err := logging.New(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		log.Fatalf("init logger failed: %v", err)
	}
	defer logger.Sync()

	// Both connections carry the same token: the query-only replica serves the
	// same listener contract as the primary, and a gateway that authenticated
	// to one but not the other would lose routing the moment enforcement lands.
	conn, err := newSchedulerConn(cfg.Gateway.SchedulerAddr, cfg.Gateway.SchedulerAuthToken)
	if err != nil {
		logger.Fatal("connect scheduler failed", zap.Error(err), zap.String("addr", cfg.Gateway.SchedulerAddr))
	}
	defer conn.Close()

	schedulerClient := schedulerv1.NewSchedulerClient(conn)
	queryOnlySchedulerClient := schedulerClient
	var queryOnlyConn *grpc.ClientConn
	if cfg.Gateway.QueryOnlySchedulerAddr != "" {
		queryOnlyConn, err = newSchedulerConn(cfg.Gateway.QueryOnlySchedulerAddr, cfg.Gateway.SchedulerAuthToken)
		if err != nil {
			logger.Fatal("connect query-only scheduler failed", zap.Error(err), zap.String("addr", cfg.Gateway.QueryOnlySchedulerAddr))
		}
		defer queryOnlyConn.Close()
		queryOnlySchedulerClient = schedulerv1.NewSchedulerClient(queryOnlyConn)
	}

	s, err := gateway.NewServer(logger, schedulerClient, serverOptionsFromConfig(cfg.Gateway, apiKey, queryOnlySchedulerClient))
	if err != nil {
		logger.Fatal("init gateway server failed", zap.Error(err))
	}

	if cfg.Gateway.SchedulerAuthToken == "" {
		// The scheduler side treats a missing token as "enforcement off", so
		// this is a warning about what a future rollout will refuse, not an
		// error about today.
		logger.Warn("gateway.scheduler_auth_token is unset; scheduler RPCs carry no credential")
	}
	logger.Info("gateway listening",
		zap.String("addr", cfg.Gateway.HTTPListenAddr),
		zap.String("metrics_addr", cfg.Gateway.MetricsListenAddr),
		zap.String("scheduler", cfg.Gateway.SchedulerAddr),
		zap.String("query_only_scheduler", cfg.Gateway.QueryOnlySchedulerAddr),
		zap.Bool("scheduler_auth", cfg.Gateway.SchedulerAuthToken != ""),
		zap.Int("binding_cache_size", cfg.Gateway.BindingCacheSize),
		zap.Strings("sandbox_proxy_domains", s.SandboxProxyDomains()),
	)
	httpServer := &http.Server{
		Addr:    cfg.Gateway.HTTPListenAddr,
		Handler: s.Handler(),
		// Without a header deadline a client that dribbles request headers
		// holds a connection and a goroutine indefinitely, which is all
		// Slowloris is. ReadTimeout and WriteTimeout are deliberately NOT set:
		// this proxy carries long-lived streams and interactive sessions, and
		// a whole-request deadline would cut them. IdleTimeout bounds
		// keep-alive connections that go quiet.
		ReadHeaderTimeout: gatewayReadHeaderTimeout,
		IdleTimeout:       gatewayIdleTimeout,
	}
	metricsServer := &http.Server{
		Addr:    cfg.Gateway.MetricsListenAddr,
		Handler: promhttp.Handler(),
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("gateway serve failed", zap.Error(err))
		}
	}()
	go func() {
		logger.Info("gateway metrics server listening", zap.String("addr", cfg.Gateway.MetricsListenAddr))
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("gateway metrics serve failed", zap.Error(err))
		}
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()

	httpShutdownCtx, cancelHTTPShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelHTTPShutdown()
	if err := httpServer.Shutdown(httpShutdownCtx); err != nil {
		logger.Warn("gateway graceful shutdown failed", zap.Error(err))
	}

	metricsShutdownCtx, cancelMetricsShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelMetricsShutdown()
	if err := metricsServer.Shutdown(metricsShutdownCtx); err != nil {
		logger.Warn("gateway metrics graceful shutdown failed", zap.Error(err))
	}
}
