package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultSchedulerArtifactStoreCapacity = 1_000_000

// defaultSchedulerArtifactLookupNodeLimit bounds how many providers one P2P
// artifact lookup names.
//
// Unlimited was the wrong default: a popular base layer is held by most of the
// fleet, and the node dials at most four candidates concurrently and stops at
// the first hit, so everything past the first few is response bytes and
// peer-filter work spent on nodes that are never contacted.
const defaultSchedulerArtifactLookupNodeLimit = 8

// defaultSchedulerReportTTL is how long a node's heartbeat keeps it a placement
// candidate when scheduler.report_ttl is unset, capped at the binding TTL.
// See applyDefaults for why it is derived rather than fixed.
const defaultSchedulerReportTTL = 30 * time.Second

// defaultSchedulerMaxReservationDelta bounds how far the reservation ledger may
// move one node's reported sandbox count before the next heartbeat corrects
// it. Events are lossy and the count is advisory; the clamp is what keeps a
// node that stops heartbeating from accumulating phantom load without bound.
const defaultSchedulerMaxReservationDelta = 512

type Node struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
}

type SchedulerDiscoveryKubernetesConfig struct {
	Namespace             string `json:"namespace"`
	ServiceName           string `json:"service_name"`
	Port                  int32  `json:"port"`
	Scheme                string `json:"scheme"`
	IgnorePodSelector     string `json:"ignore_pod_selector"`
	NoSchedulePodSelector string `json:"no_schedule_pod_selector"`
}

type SchedulerDiscoveryConfig struct {
	Mode       string                             `json:"mode"`
	Kubernetes SchedulerDiscoveryKubernetesConfig `json:"kubernetes"`
}

// NodeResourceLimit defines per-node resource thresholds for scheduling
// eligibility. A node exceeding any configured limit is excluded from
// scheduling candidates. Nil (absent) fields impose no limit.
//
// Allocated-percent limits (CPU and memory) can legitimately exceed 100%
// because allocated resources reflect the sum of all sandbox reservations,
// which may overcommit the physical capacity of the node.
type NodeResourceLimit struct {
	MaxSandboxCount           *uint32 `json:"max_sandbox_count"`
	MaxSandboxStartingCount   *uint32 `json:"max_sandbox_starting_count"`
	MaxCPUUsedPercent         *uint32 `json:"max_cpu_used_percent"`
	MaxCPUAllocatedPercent    *uint32 `json:"max_cpu_allocated_percent"` // can exceed 100 (overcommit)
	MaxMemoryUsedPercent      *uint32 `json:"max_memory_used_percent"`
	MaxMemoryAllocatedPercent *uint32 `json:"max_memory_allocated_percent"` // can exceed 100 (overcommit)

	// Limits that apply to the sum of the active running set plus paused
	// sandboxes. Paused sandboxes have released their VM-side CPU / memory
	// but still occupy persisted state on the node, so operators may want a
	// separate ceiling on total node footprint (including paused) on top of
	// the active-only ceilings above.
	MaxSandboxCountIncludingPaused         *uint32 `json:"max_sandbox_count_including_paused"`
	MaxAllocatedCPUIncludingPaused         *uint32 `json:"max_allocated_cpu_including_paused"`
	MaxAllocatedMemoryBytesIncludingPaused *uint64 `json:"max_allocated_memory_bytes_including_paused"`
}

type SchedulerConfig struct {
	GRPCListenAddr    string `json:"grpc_listen_addr"`
	MetricsListenAddr string `json:"metrics_listen_addr"`
	// Strategy picks a node from the eligible candidates: round_robin (the
	// default), random, least_loaded_of_two (alias p2c) or bin_pack.
	//
	// bin_pack fills the most loaded node that still passes every limit in
	// NodeResourceLimit. That is right for draining or consolidating a fleet
	// and wrong for tail latency: it concentrates concurrent starts on one
	// node, where they contend for the same network-slot and iptables locks,
	// so the slowest create in a burst gets slower. It is also only bounded by
	// NodeResourceLimit — with no limit configured it fills one node
	// indefinitely.
	Strategy                string                   `json:"strategy"`
	ReportTTL               time.Duration            `json:"report_ttl"`
	BindingTTL              time.Duration            `json:"binding_ttl"`
	RedisAddr               string                   `json:"redis_addr"`
	ArtifactStoreCapacity   int                      `json:"artifact_store_capacity"`
	ArtifactLookupNodeLimit int                      `json:"artifact_lookup_node_limit"`
	Nodes                   []Node                   `json:"nodes"`
	Discovery               SchedulerDiscoveryConfig `json:"discovery"`
	NodeResourceLimit       *NodeResourceLimit       `json:"node_resource_limit"`
	// ScheduleHealthGate excludes nodes whose last heartbeat is older than
	// ReportTTL, or that report themselves unhealthy or draining, from
	// placement. It defaults to enabled; set it to false to restore the
	// previous behavior of placing on any discovered node.
	ScheduleHealthGate *bool `json:"schedule_health_gate"`
	// HeartbeatInterval is the interval nodes are expected to report at. It is
	// used only to validate that the TTLs above leave room for a node to miss
	// a heartbeat and retry; zero disables that check.
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	// ReconcileGrace is how recently a binding must have been written for a
	// heartbeat reconcile to leave it alone. It covers the gap between a node
	// collecting its roster and the scheduler acting on it, during which a
	// newly placed sandbox is bound but not yet in any roster. Zero uses the
	// store default.
	ReconcileGrace time.Duration `json:"reconcile_grace"`
	// AuthToken is the shared secret every gRPC caller must present as
	// `authorization: Bearer <token>` metadata. Empty leaves the listener
	// open, which is what every deployment before this key did; the scheduler
	// says so once at startup. AuthTokenFile names a file holding the token
	// instead, for deployments that mount secrets rather than render them
	// into config. Setting both is refused rather than resolved by precedence.
	AuthToken     string `json:"auth_token"`
	AuthTokenFile string `json:"auth_token_file"`
	// ReservationsEnabled lets node-reported lifecycle events and the
	// scheduler's own placements adjust a node's last heartbeat snapshot
	// between heartbeats. Off by default: the ledger is advisory and the
	// heartbeat overwrites it, so the cost of leaving it off is that a burst
	// inside one interval reads the same numbers, and the cost of a defect in
	// it would be placements refused for capacity the fleet has.
	ReservationsEnabled bool `json:"reservations_enabled"`
	// MaxReservationDelta clamps how far the ledger may move one node's
	// sandbox count from what its last heartbeat reported. Zero uses the
	// default.
	MaxReservationDelta int `json:"max_reservation_delta"`
}

// HealthGateEnabled reports whether health-gated placement is on, defaulting
// to true when the key is absent.
func (s *SchedulerConfig) HealthGateEnabled() bool {
	return s.ScheduleHealthGate == nil || *s.ScheduleHealthGate
}

func (s *SchedulerConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		GRPCListenAddr          *string                   `json:"grpc_listen_addr"`
		MetricsListenAddr       *string                   `json:"metrics_listen_addr"`
		Strategy                *string                   `json:"strategy"`
		ReportTTL               json.RawMessage           `json:"report_ttl"`
		BindingTTL              json.RawMessage           `json:"binding_ttl"`
		RedisAddr               *string                   `json:"redis_addr"`
		ArtifactStoreCapacity   *int                      `json:"artifact_store_capacity"`
		ArtifactLookupNodeLimit *int                      `json:"artifact_lookup_node_limit"`
		Nodes                   *[]Node                   `json:"nodes"`
		Discovery               *SchedulerDiscoveryConfig `json:"discovery"`
		NodeResourceLimit       *NodeResourceLimit        `json:"node_resource_limit"`
		ScheduleHealthGate      *bool                     `json:"schedule_health_gate"`
		HeartbeatInterval       json.RawMessage           `json:"heartbeat_interval"`
		ReconcileGrace          json.RawMessage           `json:"reconcile_grace"`
		AuthToken               *string                   `json:"auth_token"`
		AuthTokenFile           *string                   `json:"auth_token_file"`
		ReservationsEnabled     *bool                     `json:"reservations_enabled"`
		MaxReservationDelta     *int                      `json:"max_reservation_delta"`
	}

	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	if parsed.GRPCListenAddr != nil {
		s.GRPCListenAddr = *parsed.GRPCListenAddr
	}
	if parsed.MetricsListenAddr != nil {
		s.MetricsListenAddr = *parsed.MetricsListenAddr
	}
	if parsed.Strategy != nil {
		s.Strategy = *parsed.Strategy
	}
	if parsed.Nodes != nil {
		s.Nodes = *parsed.Nodes
	}
	if parsed.Discovery != nil {
		s.Discovery = *parsed.Discovery
	}
	if parsed.ScheduleHealthGate != nil {
		s.ScheduleHealthGate = parsed.ScheduleHealthGate
	}
	if parsed.NodeResourceLimit != nil {
		s.NodeResourceLimit = parsed.NodeResourceLimit
	}
	if parsed.RedisAddr != nil {
		s.RedisAddr = *parsed.RedisAddr
	}
	if parsed.ArtifactStoreCapacity != nil {
		s.ArtifactStoreCapacity = *parsed.ArtifactStoreCapacity
	}
	if parsed.ArtifactLookupNodeLimit != nil {
		s.ArtifactLookupNodeLimit = *parsed.ArtifactLookupNodeLimit
	}
	if parsed.AuthToken != nil {
		s.AuthToken = *parsed.AuthToken
	}
	if parsed.AuthTokenFile != nil {
		s.AuthTokenFile = *parsed.AuthTokenFile
	}
	if parsed.ReservationsEnabled != nil {
		s.ReservationsEnabled = *parsed.ReservationsEnabled
	}
	if parsed.MaxReservationDelta != nil {
		s.MaxReservationDelta = *parsed.MaxReservationDelta
	}

	if len(bytes.TrimSpace(parsed.ReportTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.ReportTTL, "scheduler.report_ttl")
		if err != nil {
			return err
		}
		s.ReportTTL = d
	}
	if len(bytes.TrimSpace(parsed.BindingTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.BindingTTL, "scheduler.binding_ttl")
		if err != nil {
			return err
		}
		s.BindingTTL = d
	}
	if len(bytes.TrimSpace(parsed.HeartbeatInterval)) > 0 {
		d, err := parseSchedulerDuration(parsed.HeartbeatInterval, "scheduler.heartbeat_interval")
		if err != nil {
			return err
		}
		s.HeartbeatInterval = d
	}
	if len(bytes.TrimSpace(parsed.ReconcileGrace)) > 0 {
		d, err := parseSchedulerDuration(parsed.ReconcileGrace, "scheduler.reconcile_grace")
		if err != nil {
			return err
		}
		s.ReconcileGrace = d
	}

	return nil
}

func parseSchedulerDuration(raw json.RawMessage, field string) (time.Duration, error) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		d, parseErr := time.ParseDuration(strings.TrimSpace(asString))
		if parseErr != nil {
			return 0, fmt.Errorf("%s must be a duration string like \"30s\": %w", field, parseErr)
		}
		return d, nil
	}

	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return 0, fmt.Errorf("%s must be a duration string like \"30s\", got numeric value %s", field, asNumber.String())
	}

	return 0, fmt.Errorf("%s must be a duration string like \"30s\"", field)
}

// DefaultGatewayBindingCacheSize is the bound gateway.binding_cache_size takes
// when the key is not written. It is a config-layer default because zero is
// that key's off switch rather than "unset".
const DefaultGatewayBindingCacheSize = 65536

type GatewayConfig struct {
	HTTPListenAddr         string        `json:"http_listen_addr"`
	MetricsListenAddr      string        `json:"metrics_listen_addr"`
	SchedulerAddr          string        `json:"scheduler_addr"`
	QueryOnlySchedulerAddr string        `json:"query_only_scheduler_addr"`
	RequestTimeout         time.Duration `json:"request_timeout"`
	ForwardResponseSize    int64         `json:"forward_response_size"`
	SandboxProxyDomains    []string      `json:"sandbox_proxy_domains"`
	// DebugMode enables debug-only behaviors in the gateway such as exposing
	// the backend node id on proxied responses. It is off by default.
	DebugMode bool `json:"debug_mode"`
	// MaxIdleConnsPerHost bounds pooled idle upstream connections per node.
	// Zero uses the gateway default.
	MaxIdleConnsPerHost int `json:"max_idle_conns_per_host"`
	// BindingCacheTTL bounds how long a sandbox-to-node lookup is reused before
	// being re-resolved. Zero uses the gateway default; it must stay well below
	// scheduler.binding_ttl. Negative disables the cache.
	BindingCacheTTL time.Duration `json:"binding_cache_ttl"`
	// BindingCacheNegativeTTL bounds how long a lookup that found no binding is
	// reused. Zero uses the gateway default; it may not exceed BindingCacheTTL.
	BindingCacheNegativeTTL time.Duration `json:"binding_cache_negative_ttl"`
	// BindingCacheSize bounds the bindings the gateway holds locally. Unlike
	// the other bounds here it carries its default from defaultConfig rather
	// than resolving zero later, because zero is the off switch: an operator
	// who writes 0 (or a negative value) gets no cache, and one who writes
	// nothing gets the default.
	BindingCacheSize int `json:"binding_cache_size"`
	// SchedulerAuthToken is presented to the scheduler on every RPC as
	// `authorization: Bearer <token>`. Empty dials without a credential, for
	// a scheduler that does not enforce one yet.
	SchedulerAuthToken string `json:"scheduler_auth_token"`
	// MaxInFlightCreates bounds concurrent create placements per gateway. Zero
	// uses the gateway default.
	MaxInFlightCreates int `json:"max_in_flight_creates"`
	// MaxScheduleRetries bounds how many further nodes a create is offered to
	// after one refuses it. Zero uses the gateway default; a negative value
	// gives every create a single attempt.
	MaxScheduleRetries int `json:"max_schedule_retries"`
}

func (g *GatewayConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		HTTPListenAddr          *string         `json:"http_listen_addr"`
		MetricsListenAddr       *string         `json:"metrics_listen_addr"`
		SchedulerAddr           *string         `json:"scheduler_addr"`
		QueryOnlySchedulerAddr  *string         `json:"query_only_scheduler_addr"`
		RequestTimeout          json.RawMessage `json:"request_timeout"`
		ForwardResponseSize     *int64          `json:"forward_response_size"`
		SandboxProxyDomains     *[]string       `json:"sandbox_proxy_domains"`
		DebugMode               *bool           `json:"debug_mode"`
		MaxIdleConnsPerHost     *int            `json:"max_idle_conns_per_host"`
		BindingCacheTTL         json.RawMessage `json:"binding_cache_ttl"`
		BindingCacheNegativeTTL json.RawMessage `json:"binding_cache_negative_ttl"`
		BindingCacheSize        *int            `json:"binding_cache_size"`
		SchedulerAuthToken      *string         `json:"scheduler_auth_token"`
		MaxInFlightCreates      *int            `json:"max_in_flight_creates"`
		MaxScheduleRetries      *int            `json:"max_schedule_retries"`
	}

	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	if parsed.HTTPListenAddr != nil {
		g.HTTPListenAddr = *parsed.HTTPListenAddr
	}
	if parsed.MetricsListenAddr != nil {
		g.MetricsListenAddr = *parsed.MetricsListenAddr
	}
	if parsed.SchedulerAddr != nil {
		g.SchedulerAddr = *parsed.SchedulerAddr
	}
	if parsed.QueryOnlySchedulerAddr != nil {
		g.QueryOnlySchedulerAddr = *parsed.QueryOnlySchedulerAddr
	}
	if parsed.ForwardResponseSize != nil {
		g.ForwardResponseSize = *parsed.ForwardResponseSize
	}
	if parsed.SandboxProxyDomains != nil {
		g.SandboxProxyDomains = *parsed.SandboxProxyDomains
	}
	if parsed.DebugMode != nil {
		g.DebugMode = *parsed.DebugMode
	}

	if len(bytes.TrimSpace(parsed.RequestTimeout)) > 0 {
		d, err := parseGatewayRequestTimeout(parsed.RequestTimeout)
		if err != nil {
			return err
		}
		g.RequestTimeout = d
	}
	if parsed.MaxIdleConnsPerHost != nil {
		g.MaxIdleConnsPerHost = *parsed.MaxIdleConnsPerHost
	}
	if parsed.MaxInFlightCreates != nil {
		g.MaxInFlightCreates = *parsed.MaxInFlightCreates
	}
	if parsed.MaxScheduleRetries != nil {
		g.MaxScheduleRetries = *parsed.MaxScheduleRetries
	}
	if len(bytes.TrimSpace(parsed.BindingCacheTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.BindingCacheTTL, "gateway.binding_cache_ttl")
		if err != nil {
			return err
		}
		g.BindingCacheTTL = d
	}
	if len(bytes.TrimSpace(parsed.BindingCacheNegativeTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.BindingCacheNegativeTTL, "gateway.binding_cache_negative_ttl")
		if err != nil {
			return err
		}
		g.BindingCacheNegativeTTL = d
	}
	if parsed.BindingCacheSize != nil {
		g.BindingCacheSize = *parsed.BindingCacheSize
	}
	if parsed.SchedulerAuthToken != nil {
		g.SchedulerAuthToken = *parsed.SchedulerAuthToken
	}

	return nil
}

func parseGatewayRequestTimeout(raw json.RawMessage) (time.Duration, error) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		d, parseErr := time.ParseDuration(strings.TrimSpace(asString))
		if parseErr != nil {
			return 0, fmt.Errorf("gateway.request_timeout must be a duration string like \"30s\": %w", parseErr)
		}
		return d, nil
	}

	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return 0, fmt.Errorf("gateway.request_timeout must be a duration string like \"30s\", got numeric value %s", asNumber.String())
	}

	return 0, errors.New("gateway.request_timeout must be a duration string like \"30s\"")
}

type Config struct {
	Service   string          `json:"service"`
	LogLevel  string          `json:"log_level"`
	LogFormat string          `json:"log_format"`
	Scheduler SchedulerConfig `json:"scheduler"`
	Gateway   GatewayConfig   `json:"gateway"`
}

func Load(path string, service string) (Config, error) {
	return load(path, service, false)
}

func LoadScheduler(path string, queryOnly bool) (Config, error) {
	return load(path, "scheduler", queryOnly)
}

func load(path string, service string, schedulerQueryOnly bool) (Config, error) {
	cfg := defaultConfig(service)
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config file: %w", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("unmarshal config json: %w", err)
		}
	}
	if err := overrideWithEnv(&cfg); err != nil {
		return Config{}, err
	}
	cfg.Service = service
	cfg.applyDefaults()
	if err := cfg.validate(schedulerQueryOnly); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func defaultConfig(service string) Config {
	return Config{
		Service:   service,
		LogLevel:  "info",
		LogFormat: "auto",
		Scheduler: SchedulerConfig{
			GRPCListenAddr:          ":9090",
			MetricsListenAddr:       ":9101",
			Strategy:                "round_robin",
			BindingTTL:              30 * time.Second,
			ArtifactStoreCapacity:   defaultSchedulerArtifactStoreCapacity,
			ArtifactLookupNodeLimit: defaultSchedulerArtifactLookupNodeLimit,
			MaxReservationDelta:     defaultSchedulerMaxReservationDelta,
			Nodes: []Node{
				{ID: "local-node", Endpoint: "http://127.0.0.1:8000"},
			},
			Discovery: SchedulerDiscoveryConfig{
				Mode: "static",
				Kubernetes: SchedulerDiscoveryKubernetesConfig{
					Scheme: "http",
				},
			},
		},
		Gateway: GatewayConfig{
			HTTPListenAddr:      ":8080",
			MetricsListenAddr:   ":9102",
			SchedulerAddr:       "127.0.0.1:9090",
			RequestTimeout:      30 * time.Second,
			ForwardResponseSize: 4 << 20,
			SandboxProxyDomains: []string{},
			BindingCacheSize:    DefaultGatewayBindingCacheSize,
		},
	}
}

func overrideWithEnv(cfg *Config) error {
	set := func(key string, target *string) {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			*target = v
		}
	}
	set("LOG_LEVEL", &cfg.LogLevel)
	set("LOG_FORMAT", &cfg.LogFormat)
	set("SCHEDULER_GRPC_LISTEN_ADDR", &cfg.Scheduler.GRPCListenAddr)
	set("SCHEDULER_METRICS_LISTEN_ADDR", &cfg.Scheduler.MetricsListenAddr)
	set("SCHEDULER_STRATEGY", &cfg.Scheduler.Strategy)
	set("SCHEDULER_REDIS_ADDR", &cfg.Scheduler.RedisAddr)
	set("SCHEDULER_AUTH_TOKEN", &cfg.Scheduler.AuthToken)
	set("SCHEDULER_AUTH_TOKEN_FILE", &cfg.Scheduler.AuthTokenFile)
	set("GATEWAY_HTTP_LISTEN_ADDR", &cfg.Gateway.HTTPListenAddr)
	set("GATEWAY_METRICS_LISTEN_ADDR", &cfg.Gateway.MetricsListenAddr)
	set("GATEWAY_SCHEDULER_ADDR", &cfg.Gateway.SchedulerAddr)
	set("GATEWAY_QUERY_ONLY_SCHEDULER_ADDR", &cfg.Gateway.QueryOnlySchedulerAddr)

	if v := strings.TrimSpace(os.Getenv("GATEWAY_SANDBOX_PROXY_DOMAINS")); v != "" {
		cfg.Gateway.SandboxProxyDomains = splitCommaSeparated(v)
	}

	for _, override := range []struct {
		key    string
		target *time.Duration
	}{
		{"SCHEDULER_BINDING_TTL", &cfg.Scheduler.BindingTTL},
		{"SCHEDULER_RECONCILE_GRACE", &cfg.Scheduler.ReconcileGrace},
		{"SCHEDULER_HEARTBEAT_INTERVAL", &cfg.Scheduler.HeartbeatInterval},
	} {
		v := strings.TrimSpace(os.Getenv(override.key))
		if v == "" {
			continue
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid %s %q: %w", override.key, v, err)
		}
		*override.target = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_ARTIFACT_STORE_CAPACITY")); v != "" {
		capacity, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_ARTIFACT_STORE_CAPACITY %q: %w", v, err)
		}
		cfg.Scheduler.ArtifactStoreCapacity = capacity
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT")); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT %q: %w", v, err)
		}
		cfg.Scheduler.ArtifactLookupNodeLimit = limit
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_RESERVATIONS_ENABLED")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_RESERVATIONS_ENABLED %q: %w", v, err)
		}
		cfg.Scheduler.ReservationsEnabled = b
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_MAX_RESERVATION_DELTA")); v != "" {
		delta, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_MAX_RESERVATION_DELTA %q: %w", v, err)
		}
		cfg.Scheduler.MaxReservationDelta = delta
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_REQUEST_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_REQUEST_TIMEOUT %q: %w", v, err)
		}
		cfg.Gateway.RequestTimeout = d
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_DEBUG_MODE")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_DEBUG_MODE %q: %w", v, err)
		}
		cfg.Gateway.DebugMode = b
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_MAX_IN_FLIGHT_CREATES")); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_MAX_IN_FLIGHT_CREATES %q: %w", v, err)
		}
		cfg.Gateway.MaxInFlightCreates = limit
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_MAX_SCHEDULE_RETRIES")); v != "" {
		retries, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_MAX_SCHEDULE_RETRIES %q: %w", v, err)
		}
		cfg.Gateway.MaxScheduleRetries = retries
	}

	for _, override := range []struct {
		key    string
		target *time.Duration
	}{
		{"GATEWAY_BINDING_CACHE_TTL", &cfg.Gateway.BindingCacheTTL},
		{"GATEWAY_BINDING_CACHE_NEGATIVE_TTL", &cfg.Gateway.BindingCacheNegativeTTL},
	} {
		v := strings.TrimSpace(os.Getenv(override.key))
		if v == "" {
			continue
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid %s %q: %w", override.key, v, err)
		}
		*override.target = d
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_BINDING_CACHE_SIZE")); v != "" {
		size, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_BINDING_CACHE_SIZE %q: %w", v, err)
		}
		cfg.Gateway.BindingCacheSize = size
	}

	set("GATEWAY_SCHEDULER_AUTH_TOKEN", &cfg.Gateway.SchedulerAuthToken)

	return nil
}

func splitCommaSeparated(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}

func (c *Config) applyDefaults() {
	if strings.TrimSpace(c.Scheduler.MetricsListenAddr) == "" {
		c.Scheduler.MetricsListenAddr = ":9101"
	}
	// Exactly zero is unset. A negative duration is an explicit value — the
	// operator wrote it, or a template rendered it — and is left for validate
	// to refuse rather than silently replaced with the default.
	if c.Scheduler.BindingTTL == 0 {
		c.Scheduler.BindingTTL = 30 * time.Second
	}
	// An unset report TTL follows the binding TTL down, the way an unset
	// reconcile grace does: validate refuses a report TTL above the binding
	// TTL, and substituting a fixed 30s under a shorter binding TTL the
	// operator did write would refuse to boot over a key they never wrote.
	if c.Scheduler.ReportTTL == 0 {
		c.Scheduler.ReportTTL = defaultSchedulerReportTTL
		if c.Scheduler.BindingTTL > 0 && c.Scheduler.BindingTTL < defaultSchedulerReportTTL {
			c.Scheduler.ReportTTL = c.Scheduler.BindingTTL
		}
	}
	if c.Scheduler.MaxReservationDelta == 0 {
		c.Scheduler.MaxReservationDelta = defaultSchedulerMaxReservationDelta
	}
	if strings.TrimSpace(c.Scheduler.Discovery.Mode) == "" {
		c.Scheduler.Discovery.Mode = "static"
	}
	if strings.TrimSpace(c.Scheduler.Discovery.Kubernetes.Scheme) == "" {
		c.Scheduler.Discovery.Kubernetes.Scheme = "http"
	}
	if strings.TrimSpace(c.Gateway.MetricsListenAddr) == "" {
		c.Gateway.MetricsListenAddr = ":9102"
	}
}

func (c Config) Validate() error {
	return c.validate(false)
}

func (c Config) validate(schedulerQueryOnly bool) error {
	if c.Service == "" {
		return errors.New("service is required")
	}
	if c.LogLevel == "" {
		return errors.New("log_level is required")
	}
	if c.LogFormat == "" {
		return errors.New("log_format is required")
	}
	switch strings.ToLower(c.LogFormat) {
	case "auto", "console", "json":
	default:
		return errors.New("log_format must be one of auto, console, json")
	}
	if c.Service == "scheduler" {
		if c.Scheduler.GRPCListenAddr == "" {
			return errors.New("scheduler.grpc_listen_addr is required")
		}
		if c.Scheduler.MetricsListenAddr == "" {
			return errors.New("scheduler.metrics_listen_addr is required")
		}
		if c.Scheduler.ReportTTL <= 0 {
			return errors.New("scheduler.report_ttl must be greater than zero")
		}
		if c.Scheduler.BindingTTL <= 0 {
			return errors.New("scheduler.binding_ttl must be greater than zero")
		}
		if c.Scheduler.HeartbeatInterval < 0 {
			return errors.New("scheduler.heartbeat_interval must not be negative")
		}
		if c.Scheduler.MaxReservationDelta < 0 {
			return errors.New("scheduler.max_reservation_delta must not be negative")
		}
		if strings.TrimSpace(c.Scheduler.AuthToken) != "" && strings.TrimSpace(c.Scheduler.AuthTokenFile) != "" {
			return errors.New("scheduler.auth_token and scheduler.auth_token_file are both set; configure one")
		}
		if err := validateSchedulerTTLOrdering(c.Scheduler); err != nil {
			return err
		}
		// Checked here as well as when the store is built, so a bad relation
		// is a config error at load time rather than a store failure at
		// startup — including for a query-only scheduler, which builds the
		// same store from the same config and never reconciles with it.
		if _, err := CheckReconcileGrace(c.Scheduler.BindingTTL, c.Scheduler.ReconcileGrace, c.Scheduler.HeartbeatInterval); err != nil {
			return err
		}
		if schedulerQueryOnly {
			if strings.TrimSpace(c.Scheduler.RedisAddr) == "" {
				return errors.New("scheduler --query-only requires scheduler.redis_addr")
			}
			return nil
		}
		if c.Scheduler.ArtifactStoreCapacity <= 0 {
			return errors.New("scheduler.artifact_store_capacity must be greater than zero")
		}
		switch strings.ToLower(strings.TrimSpace(c.Scheduler.Discovery.Mode)) {
		case "static":
			if len(c.Scheduler.Nodes) == 0 {
				return errors.New("scheduler.nodes must not be empty")
			}
			for _, n := range c.Scheduler.Nodes {
				if n.ID == "" || n.Endpoint == "" {
					return errors.New("scheduler.nodes require id and endpoint")
				}
			}
		case "kubernetes":
			kube := c.Scheduler.Discovery.Kubernetes
			if strings.TrimSpace(kube.Namespace) == "" {
				return errors.New("scheduler.discovery.kubernetes.namespace is required")
			}
			if strings.TrimSpace(kube.ServiceName) == "" {
				return errors.New("scheduler.discovery.kubernetes.service_name is required")
			}
			if kube.Port <= 0 {
				return errors.New("scheduler.discovery.kubernetes.port must be greater than zero")
			}
			if strings.TrimSpace(kube.Scheme) == "" {
				return errors.New("scheduler.discovery.kubernetes.scheme is required")
			}
		default:
			return errors.New("scheduler.discovery.mode must be one of static, kubernetes")
		}
	}
	if c.Service == "gateway" {
		if c.Gateway.HTTPListenAddr == "" {
			return errors.New("gateway.http_listen_addr is required")
		}
		if c.Gateway.MetricsListenAddr == "" {
			return errors.New("gateway.metrics_listen_addr is required")
		}
		if c.Gateway.SchedulerAddr == "" {
			return errors.New("gateway.scheduler_addr is required")
		}
		if c.Gateway.BindingCacheNegativeTTL < 0 {
			return errors.New("gateway.binding_cache_negative_ttl must not be negative; disable the cache with gateway.binding_cache_size = 0")
		}
		if c.Gateway.BindingCacheTTL > 0 && c.Gateway.BindingCacheNegativeTTL > c.Gateway.BindingCacheTTL {
			return fmt.Errorf(
				"gateway.binding_cache_negative_ttl (%s) must not exceed gateway.binding_cache_ttl (%s); "+
					"a cached \"not found\" that outlives a cached binding turns a create's first request into a 404",
				c.Gateway.BindingCacheNegativeTTL, c.Gateway.BindingCacheTTL,
			)
		}
	}
	return nil
}

// minHeartbeatsBeforeExpiry is how many consecutive heartbeats a node may lose
// before its state is considered gone.
//
// One is not enough: a single dropped packet or a brief scheduler pause would
// expire a healthy node's bindings and drop its placement eligibility. Two
// gives the node a retry before anything is torn down.
const minHeartbeatsBeforeExpiry = 3

// MinTTLForHeartbeatInterval is the shortest TTL that still leaves a node room
// to miss a heartbeat and retry before its state is torn down. A
// non-positive interval returns zero, which every TTL satisfies.
//
// Exported because the ordering has to be checked twice. Here it is checked
// once at startup against scheduler.heartbeat_interval, which is optional and
// which no shipped config sets — so on a real deployment this check does not
// run at all. The scheduler re-checks the same relation on every heartbeat
// against the interval the node actually reports, and both need to agree on
// what "long enough" means.
func MinTTLForHeartbeatInterval(interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	return time.Duration(minHeartbeatsBeforeExpiry) * interval
}

// DefaultReconcileGrace is how recently a binding must have been written for a
// heartbeat reconcile to leave it alone even when the reporting node's roster
// omits it, when scheduler.reconcile_grace is unset.
//
// The window it has to cover is roster collection through heartbeat apply, and
// it has to cover it with margin rather than merely reach it: a binding one
// tick older than the grace is deleted while its sandbox is still running, with
// no migration, no error and nothing in a log. Nothing in the request path can
// measure that margin, which is why the relations below are checked at load
// time and again when the store is built, and why every reconcile delete and
// retention is counted per node.
const DefaultReconcileGrace = 10 * time.Second

// MinReconcileGraceHeartbeats is the smallest number of reporting intervals a
// grace period may span.
//
// One interval covers a report that lands first time and nothing else. When the
// report carrying a roster is dropped, the next roster is collected a full
// interval later, so a binding written just after the first collection is that
// much older by the time a delete is considered. Two intervals is the smallest
// bound that still holds across one retry.
const MinReconcileGraceHeartbeats = 2

// ResolveReconcileGrace returns the grace a store built from these timings
// runs with: the configured one when set, otherwise the default, capped at
// half the binding TTL.
//
// The cap is what lets a short binding TTL be configured without also writing
// a grace. Substituting the default blindly made binding_ttl at or below 10s
// fail the grace-below-TTL relation at startup — over a key the operator had
// never written — and a deployment that had booted before the relation was
// checked crashlooped on upgrade. Half the TTL always satisfies that relation.
// The interval relation is still checked when an interval is configured, and
// CheckReconcileGrace names the derivation when it is the derived value that
// fails it.
func ResolveReconcileGrace(bindingTTL time.Duration, reconcileGrace time.Duration) time.Duration {
	if reconcileGrace != 0 {
		return reconcileGrace
	}
	if bindingTTL > 0 && bindingTTL/2 < DefaultReconcileGrace {
		return bindingTTL / 2
	}
	return DefaultReconcileGrace
}

// ValidateReconcileGrace checks the timing relations the grace period depends on
// but that nothing states or measures.
//
// heartbeatInterval is the interval nodes are expected to report at. Zero means
// no expectation is configured, which is the shipped case, and leaves the
// grace-versus-interval relation unchecked because there is nothing to check
// it against.
func ValidateReconcileGrace(bindingTTL time.Duration, reconcileGrace time.Duration, heartbeatInterval time.Duration) error {
	if bindingTTL <= 0 {
		return fmt.Errorf("scheduler.binding_ttl (%s) must be greater than zero", bindingTTL)
	}
	if reconcileGrace < 0 {
		return fmt.Errorf("scheduler.reconcile_grace (%s) must not be negative", reconcileGrace)
	}
	if reconcileGrace >= bindingTTL {
		return fmt.Errorf(
			"scheduler.reconcile_grace (%s) must be shorter than scheduler.binding_ttl (%s); "+
				"a grace that reaches the ttl protects a binding for longer than one ttl, "+
				"so a sandbox created and deleted inside a single ttl can only be reaped by expiry",
			reconcileGrace, bindingTTL,
		)
	}
	if heartbeatInterval <= 0 {
		return nil
	}
	minimum := time.Duration(MinReconcileGraceHeartbeats) * heartbeatInterval
	if reconcileGrace < minimum {
		return fmt.Errorf(
			"scheduler.reconcile_grace (%s) must be at least %d heartbeat intervals (%s); "+
				"a shorter grace deletes bindings written while the reporting node was still collecting the roster that omits them",
			reconcileGrace, MinReconcileGraceHeartbeats, minimum,
		)
	}
	return nil
}

// CheckReconcileGrace resolves the grace and validates it, and returns the
// value a store should run with.
//
// When the grace was derived rather than written, the error says so and names
// the derivation: the relation that failed is between two keys the operator
// set, and an error citing a third they never wrote sends them to the wrong
// knob.
func CheckReconcileGrace(bindingTTL time.Duration, reconcileGrace time.Duration, heartbeatInterval time.Duration) (time.Duration, error) {
	grace := ResolveReconcileGrace(bindingTTL, reconcileGrace)
	if err := ValidateReconcileGrace(bindingTTL, grace, heartbeatInterval); err != nil {
		if reconcileGrace == 0 {
			return 0, fmt.Errorf(
				"scheduler.reconcile_grace is unset and defaults to %s, the smaller of %s and half of scheduler.binding_ttl: %w",
				grace, DefaultReconcileGrace, err,
			)
		}
		return 0, err
	}
	return grace, nil
}

// validateSchedulerTTLOrdering checks the timing relations the control plane
// depends on but never stated.
//
// These are the relations that decide whether a transient blip looks like a
// dead node. They were previously implicit, and the shipped defaults violate
// them: with a 5s node interval, a 30s TTL leaves only two heartbeats of slack
// while the node's own retry backoff can exceed 30s, so the second retry lands
// after expiry. An implicit relation that is already false is worth turning
// into a startup error rather than an intermittent outage.
func validateSchedulerTTLOrdering(cfg SchedulerConfig) error {
	// A node stops receiving placements when its heartbeat is older than
	// report_ttl and loses its routing when a binding is older than
	// binding_ttl. Routing must outlive placement: a node that missed two
	// heartbeats but is still running its sandboxes is still the only place
	// they can be reached, and expiring its bindings while it is still healthy
	// enough to receive new work 404s live sandboxes on a node the scheduler
	// is simultaneously filling. The shipped defaults are equal.
	if cfg.ReportTTL > cfg.BindingTTL {
		return fmt.Errorf(
			"scheduler.report_ttl (%s) must not exceed scheduler.binding_ttl (%s); "+
				"a node's bindings would expire while it is still a placement candidate",
			cfg.ReportTTL, cfg.BindingTTL,
		)
	}
	minimum := MinTTLForHeartbeatInterval(cfg.HeartbeatInterval)
	if minimum <= 0 {
		// Nodes report their interval on every heartbeat; without a configured
		// expectation there is nothing to check here.
		return nil
	}
	if cfg.ReportTTL < minimum {
		return fmt.Errorf(
			"scheduler.report_ttl (%s) must be at least %d heartbeat intervals (%s); "+
				"a shorter TTL marks a healthy node stale after a single missed heartbeat",
			cfg.ReportTTL, minHeartbeatsBeforeExpiry, minimum,
		)
	}
	if cfg.BindingTTL < minimum {
		return fmt.Errorf(
			"scheduler.binding_ttl (%s) must be at least %d heartbeat intervals (%s); "+
				"a shorter TTL drops a healthy node's bindings after a single missed heartbeat",
			cfg.BindingTTL, minHeartbeatsBeforeExpiry, minimum,
		)
	}
	return nil
}
