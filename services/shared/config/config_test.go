package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigUsesAutoLogFormat(t *testing.T) {
	cfg := defaultConfig("gateway")
	if cfg.LogFormat != "auto" {
		t.Fatalf("expected default log format auto, got %q", cfg.LogFormat)
	}
}

func TestDefaultSchedulerDiscoveryModeIsStatic(t *testing.T) {
	cfg := defaultConfig("scheduler")
	if got := cfg.Scheduler.Discovery.Mode; got != "static" {
		t.Fatalf("expected scheduler discovery mode static, got %q", got)
	}
	if got := cfg.Scheduler.Discovery.Kubernetes.Scheme; got != "http" {
		t.Fatalf("expected kubernetes discovery scheme http, got %q", got)
	}
	if got := cfg.Scheduler.ReportTTL; got != 30*time.Second {
		t.Fatalf("expected scheduler report ttl 30s, got %s", got)
	}
	if got := cfg.Scheduler.BindingTTL; got != 30*time.Second {
		t.Fatalf("expected scheduler binding ttl 30s, got %s", got)
	}
	if got := cfg.Scheduler.MetricsListenAddr; got != ":9101" {
		t.Fatalf("expected scheduler metrics listen addr :9101, got %q", got)
	}
	if got := cfg.Scheduler.ArtifactStoreCapacity; got != defaultSchedulerArtifactStoreCapacity {
		t.Fatalf("expected scheduler artifact store capacity %d, got %d", defaultSchedulerArtifactStoreCapacity, got)
	}
	if got := cfg.Scheduler.ArtifactLookupNodeLimit; got != 0 {
		t.Fatalf("expected scheduler artifact lookup node limit 0, got %d", got)
	}
}

func TestLoadSchedulerAllowsQueryOnlyWithRedisWithoutNodes(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"redis_addr": "127.0.0.1:6379",
			"nodes": []
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := LoadScheduler(path, true)
	if err != nil {
		t.Fatalf("load query-only scheduler config failed: %v", err)
	}
	if cfg.Scheduler.RedisAddr != "127.0.0.1:6379" {
		t.Fatalf("unexpected redis addr: %q", cfg.Scheduler.RedisAddr)
	}
}

func TestLoadSchedulerRejectsQueryOnlyWithoutRedis(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"nodes": []
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	if _, err := LoadScheduler(path, true); err == nil {
		t.Fatal("expected query-only scheduler without redis_addr to fail")
	}
}

func TestValidateRejectsUnsupportedLogFormat(t *testing.T) {
	cfg := defaultConfig("gateway")
	cfg.LogFormat = "pretty"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validate to reject unsupported log_format")
	}
}

func TestValidateAcceptsSupportedLogFormats(t *testing.T) {
	formats := []string{"auto", "console", "json"}
	for _, format := range formats {
		cfg := defaultConfig("gateway")
		cfg.LogFormat = format
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected format %q to validate, got error %v", format, err)
		}
	}
}

func TestLoadParsesGatewayRequestTimeoutDurationString(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"gateway": {
			"request_timeout": "45s",
			"sandbox_proxy_domains": ["sandbox-proxy.example.invalid", "sandbox-proxy-alt.example.invalid"]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.RequestTimeout != 45*time.Second {
		t.Fatalf("expected request timeout 45s, got %s", cfg.Gateway.RequestTimeout)
	}
	if got := cfg.Gateway.SandboxProxyDomains; len(got) != 2 || got[0] != "sandbox-proxy.example.invalid" || got[1] != "sandbox-proxy-alt.example.invalid" {
		t.Fatalf("unexpected proxy domains: %#v", got)
	}
}

func TestLoadRejectsNumericGatewayRequestTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"gateway": {
			"request_timeout": 30
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	_, err := Load(path, "gateway")
	if err == nil {
		t.Fatal("expected load to fail for numeric request_timeout")
	}
}

func TestLoadAppliesGatewayRequestTimeoutEnvDuration(t *testing.T) {
	t.Setenv("GATEWAY_REQUEST_TIMEOUT", "1m30s")
	t.Setenv("GATEWAY_SANDBOX_PROXY_DOMAINS", " sandbox-proxy.example.invalid,sandbox-proxy-alt.example.invalid ,,")

	cfg, err := Load("", "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.RequestTimeout != 90*time.Second {
		t.Fatalf("expected request timeout 90s, got %s", cfg.Gateway.RequestTimeout)
	}
	if got := cfg.Gateway.SandboxProxyDomains; len(got) != 2 || got[0] != "sandbox-proxy.example.invalid" || got[1] != "sandbox-proxy-alt.example.invalid" {
		t.Fatalf("unexpected proxy domains from env: %#v", got)
	}
}

func TestLoadRejectsInvalidGatewayRequestTimeoutEnvDuration(t *testing.T) {
	t.Setenv("GATEWAY_REQUEST_TIMEOUT", "1m30")

	_, err := Load("", "gateway")
	if err == nil {
		t.Fatal("expected load to fail for invalid GATEWAY_REQUEST_TIMEOUT")
	}
}

func TestLoadDefaultsSchedulerDiscoveryToStaticWhenUnset(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.Discovery.Mode; got != "static" {
		t.Fatalf("expected discovery mode static, got %q", got)
	}
}

func TestLoadParsesKubernetesSchedulerDiscoveryConfig(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"discovery": {
				"mode": "kubernetes",
				"kubernetes": {
					"namespace": "agentenv-system",
					"service_name": "agentenv-nodes",
					"port": 8000,
					"ignore_pod_selector": "agentenv.io/discovery=ignore",
					"no_schedule_pod_selector": "agentenv.io/scheduler-state in (draining,no-schedule)"
				}
			}
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.Discovery.Mode; got != "kubernetes" {
		t.Fatalf("expected discovery mode kubernetes, got %q", got)
	}
	if got := cfg.Scheduler.Discovery.Kubernetes.Scheme; got != "http" {
		t.Fatalf("expected default discovery scheme http, got %q", got)
	}
	if got := cfg.Scheduler.Discovery.Kubernetes.Namespace; got != "agentenv-system" {
		t.Fatalf("expected namespace agentenv-system, got %q", got)
	}
	if got := cfg.Scheduler.Discovery.Kubernetes.IgnorePodSelector; got != "agentenv.io/discovery=ignore" {
		t.Fatalf("expected ignore pod selector, got %q", got)
	}
	if got := cfg.Scheduler.Discovery.Kubernetes.NoSchedulePodSelector; got != "agentenv.io/scheduler-state in (draining,no-schedule)" {
		t.Fatalf("expected no-schedule pod selector, got %q", got)
	}
}

func TestLoadDefaultsSchedulerReportTTLWhenUnset(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ReportTTL; got != 30*time.Second {
		t.Fatalf("expected scheduler report ttl default 30s, got %s", got)
	}
	if got := cfg.Scheduler.BindingTTL; got != 30*time.Second {
		t.Fatalf("expected scheduler binding ttl default 30s, got %s", got)
	}
}

func TestLoadParsesSchedulerReportTTLDurationString(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"report_ttl": "45s",
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ReportTTL; got != 45*time.Second {
		t.Fatalf("expected scheduler report ttl 45s, got %s", got)
	}
}

func TestLoadParsesSchedulerBindingTTLDurationString(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"binding_ttl": "75s",
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.BindingTTL; got != 75*time.Second {
		t.Fatalf("expected scheduler binding ttl 75s, got %s", got)
	}
}

func TestLoadParsesSchedulerArtifactStoreCapacity(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"artifact_store_capacity": 42,
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ArtifactStoreCapacity; got != 42 {
		t.Fatalf("expected scheduler artifact store capacity 42, got %d", got)
	}
}

func TestLoadParsesSchedulerArtifactLookupNodeLimit(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"artifact_lookup_node_limit": 7,
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ArtifactLookupNodeLimit; got != 7 {
		t.Fatalf("expected scheduler artifact lookup node limit 7, got %d", got)
	}
}

func TestLoadAllowsNonPositiveSchedulerArtifactLookupNodeLimit(t *testing.T) {
	for _, limit := range []int{0, -1} {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "config.json")
		content := fmt.Sprintf(`{
			"scheduler": {
				"artifact_lookup_node_limit": %d,
				"nodes": [
					{"id": "node-a", "endpoint": "http://node-a:8000"}
				]
			}
		}`, limit)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write config file failed: %v", err)
		}

		cfg, err := Load(path, "scheduler")
		if err != nil {
			t.Fatalf("load config failed for limit %d: %v", limit, err)
		}
		if got := cfg.Scheduler.ArtifactLookupNodeLimit; got != limit {
			t.Fatalf("expected scheduler artifact lookup node limit %d, got %d", limit, got)
		}
	}
}

func TestLoadRejectsNonIntegerSchedulerArtifactLookupNodeLimit(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"artifact_lookup_node_limit": "many",
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	_, err := Load(path, "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for non-integer scheduler.artifact_lookup_node_limit")
	}
}

func TestLoadRejectsNonPositiveSchedulerArtifactStoreCapacity(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"artifact_store_capacity": 0,
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	_, err := Load(path, "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for non-positive scheduler.artifact_store_capacity")
	}
}

func TestLoadRejectsNonIntegerSchedulerArtifactStoreCapacity(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"artifact_store_capacity": "many",
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	_, err := Load(path, "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for non-integer scheduler.artifact_store_capacity")
	}
}

func TestLoadRejectsNumericSchedulerReportTTL(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"report_ttl": 30,
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	_, err := Load(path, "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for numeric scheduler.report_ttl")
	}
}

func TestLoadRejectsNumericSchedulerBindingTTL(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"binding_ttl": 30,
			"nodes": [
				{"id": "node-a", "endpoint": "http://node-a:8000"}
			]
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	_, err := Load(path, "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for numeric scheduler.binding_ttl")
	}
}

func TestLoadAppliesSchedulerBindingTTLEnvDuration(t *testing.T) {
	t.Setenv("SCHEDULER_BINDING_TTL", "45s")

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Scheduler.BindingTTL != 45*time.Second {
		t.Fatalf("expected binding ttl 45s, got %s", cfg.Scheduler.BindingTTL)
	}
}

func TestLoadAppliesSchedulerArtifactStoreCapacityEnv(t *testing.T) {
	t.Setenv("SCHEDULER_ARTIFACT_STORE_CAPACITY", "123")

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ArtifactStoreCapacity; got != 123 {
		t.Fatalf("expected artifact store capacity 123, got %d", got)
	}
}

func TestLoadAppliesSchedulerArtifactLookupNodeLimitEnv(t *testing.T) {
	t.Setenv("SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT", "9")

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ArtifactLookupNodeLimit; got != 9 {
		t.Fatalf("expected artifact lookup node limit 9, got %d", got)
	}
}

func TestLoadAllowsNonPositiveSchedulerArtifactLookupNodeLimitEnv(t *testing.T) {
	t.Setenv("SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT", "-1")

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.ArtifactLookupNodeLimit; got != -1 {
		t.Fatalf("expected artifact lookup node limit -1, got %d", got)
	}
}

func TestLoadRejectsInvalidSchedulerArtifactLookupNodeLimitEnv(t *testing.T) {
	t.Setenv("SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT", "many")

	_, err := Load("", "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for invalid SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT")
	}
}

func TestLoadRejectsInvalidSchedulerArtifactStoreCapacityEnv(t *testing.T) {
	t.Setenv("SCHEDULER_ARTIFACT_STORE_CAPACITY", "many")

	_, err := Load("", "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for invalid SCHEDULER_ARTIFACT_STORE_CAPACITY")
	}
}

func TestLoadRejectsNonPositiveSchedulerArtifactStoreCapacityEnv(t *testing.T) {
	t.Setenv("SCHEDULER_ARTIFACT_STORE_CAPACITY", "0")

	_, err := Load("", "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for non-positive SCHEDULER_ARTIFACT_STORE_CAPACITY")
	}
}

func TestLoadRejectsInvalidSchedulerBindingTTLEnvDuration(t *testing.T) {
	t.Setenv("SCHEDULER_BINDING_TTL", "45")

	_, err := Load("", "scheduler")
	if err == nil {
		t.Fatal("expected load to fail for invalid SCHEDULER_BINDING_TTL")
	}
}

func TestLoadAppliesSchedulerMetricsListenAddrEnv(t *testing.T) {
	t.Setenv("SCHEDULER_METRICS_LISTEN_ADDR", ":19101")

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Scheduler.MetricsListenAddr != ":19101" {
		t.Fatalf("expected scheduler metrics listen addr :19101, got %q", cfg.Scheduler.MetricsListenAddr)
	}
}

func TestLoadRejectsIncompleteKubernetesSchedulerDiscoveryConfig(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"scheduler": {
			"discovery": {
				"mode": "kubernetes",
				"kubernetes": {
					"namespace": "agentenv-system",
					"service_name": "",
					"port": 0
				}
			}
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	if _, err := Load(path, "scheduler"); err == nil {
		t.Fatal("expected load to fail for incomplete kubernetes discovery config")
	}
}

func TestValidateSchedulerTTLOrdering(t *testing.T) {
	cases := []struct {
		name    string
		cfg     SchedulerConfig
		wantErr bool
	}{
		{
			name: "no expected interval disables the check",
			cfg:  SchedulerConfig{ReportTTL: time.Second, BindingTTL: time.Second},
		},
		{
			name: "ttls leave room for a missed heartbeat and a retry",
			cfg: SchedulerConfig{
				HeartbeatInterval: 5 * time.Second,
				ReportTTL:         30 * time.Second,
				BindingTTL:        30 * time.Second,
			},
		},
		{
			// A single missed heartbeat would mark a healthy node stale.
			name: "report ttl too short for the interval",
			cfg: SchedulerConfig{
				HeartbeatInterval: 5 * time.Second,
				ReportTTL:         6 * time.Second,
				BindingTTL:        30 * time.Second,
			},
			wantErr: true,
		},
		{
			// A single missed heartbeat would drop the node's bindings.
			name: "binding ttl too short for the interval",
			cfg: SchedulerConfig{
				HeartbeatInterval: 5 * time.Second,
				ReportTTL:         30 * time.Second,
				BindingTTL:        10 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "exactly at the minimum is accepted",
			cfg: SchedulerConfig{
				HeartbeatInterval: 5 * time.Second,
				ReportTTL:         15 * time.Second,
				BindingTTL:        15 * time.Second,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSchedulerTTLOrdering(tc.cfg)
			if tc.wantErr && err == nil {
				t.Fatal("expected a validation error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func writeSchedulerConfig(t *testing.T, scheduler string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	content := fmt.Sprintf(`{"scheduler": {%s, "nodes": [{"id": "node-a", "endpoint": "http://node-a:8000"}]}}`, scheduler)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}
	return path
}

// A negative duration is an explicit value, not an absent one. It used to be
// replaced with the 30s default before validation ran, so validate's
// greater-than-zero checks were unreachable from a file or the environment and
// an operator's typo booted a healthy scheduler on numbers the config did not
// say. Zero stays the unset marker.
func TestLoadRejectsNegativeSchedulerTTLs(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{name: "binding ttl", body: `"binding_ttl": "-5s"`, want: "scheduler.binding_ttl must be greater than zero"},
		{name: "report ttl", body: `"report_ttl": "-1h"`, want: "scheduler.report_ttl must be greater than zero"},
		{name: "reconcile grace", body: `"reconcile_grace": "-1s"`, want: "scheduler.reconcile_grace (-1s) must not be negative"},
		{name: "heartbeat interval", body: `"heartbeat_interval": "-5s"`, want: "scheduler.heartbeat_interval must not be negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeSchedulerConfig(t, tc.body), "scheduler")
			if err == nil {
				t.Fatal("a negative duration loaded")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}

	cfg, err := Load(writeSchedulerConfig(t, `"binding_ttl": "0s"`), "scheduler")
	if err != nil {
		t.Fatalf("an explicit zero must still read as unset: %v", err)
	}
	if cfg.Scheduler.BindingTTL != 30*time.Second {
		t.Fatalf("binding ttl = %s, want the 30s default for an explicit zero", cfg.Scheduler.BindingTTL)
	}
}

func TestLoadRejectsNegativeSchedulerBindingTTLEnv(t *testing.T) {
	t.Setenv("SCHEDULER_BINDING_TTL", "-5s")

	_, err := Load("", "scheduler")
	if err == nil {
		t.Fatal("a negative SCHEDULER_BINDING_TTL loaded")
	}
	if !strings.Contains(err.Error(), "scheduler.binding_ttl must be greater than zero") {
		t.Fatalf("error = %q, want the greater-than-zero refusal", err)
	}
}

// The grace relations used to be checked only when the binding store was built,
// so a config that passed Load died at startup as a store error, and did so in
// the query-only scheduler too, which never reconciles. Load checks them now,
// in both modes, naming the config keys.
func TestLoadRejectsMisorderedReconcileGrace(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "grace at or past the binding ttl",
			body: `"binding_ttl": "30s", "reconcile_grace": "40s"`,
			want: "scheduler.reconcile_grace (40s) must be shorter than scheduler.binding_ttl (30s)",
		},
		{
			name: "grace shorter than two heartbeat intervals",
			body: `"binding_ttl": "30s", "heartbeat_interval": "5s", "reconcile_grace": "6s"`,
			want: "scheduler.reconcile_grace (6s) must be at least 2 heartbeat intervals (10s)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeSchedulerConfig(t, tc.body)
			_, err := LoadScheduler(path, false)
			if err == nil {
				t.Fatal("a misordered grace loaded")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.want)
			}

			queryOnly := writeSchedulerConfig(t, tc.body+`, "redis_addr": "127.0.0.1:6379"`)
			if _, err := LoadScheduler(queryOnly, true); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("query-only load: err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// Setting only binding_ttl must load, whatever the value, because the grace
// follows it down. A 10s TTL with the grace unset used to be refused for
// reaching the 10s default the operator never wrote.
func TestLoadDerivesTheReconcileGraceFromAShortBindingTTL(t *testing.T) {
	cfg, err := LoadScheduler(writeSchedulerConfig(t, `"binding_ttl": "10s"`), false)
	if err != nil {
		t.Fatalf("a 10s binding ttl with the grace unset was refused: %v", err)
	}
	if cfg.Scheduler.ReconcileGrace != 0 {
		t.Fatalf("reconcile grace = %s in the loaded config, want it left unset for the store to derive", cfg.Scheduler.ReconcileGrace)
	}
	grace, err := CheckReconcileGrace(cfg.Scheduler.BindingTTL, cfg.Scheduler.ReconcileGrace, cfg.Scheduler.HeartbeatInterval)
	if err != nil {
		t.Fatalf("the derived grace failed its own check: %v", err)
	}
	if grace != 5*time.Second {
		t.Fatalf("derived grace = %s, want half the ttl", grace)
	}
}

func TestResolveReconcileGrace(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ttl   time.Duration
		grace time.Duration
		want  time.Duration
	}{
		{name: "explicit grace is kept", ttl: 30 * time.Second, grace: 12 * time.Second, want: 12 * time.Second},
		{name: "default fits under a long ttl", ttl: 30 * time.Second, want: DefaultReconcileGrace},
		{name: "default fits exactly at twice its length", ttl: 20 * time.Second, want: DefaultReconcileGrace},
		{name: "a short ttl halves", ttl: 10 * time.Second, want: 5 * time.Second},
		{name: "a very short ttl halves", ttl: 3 * time.Second, want: 1500 * time.Millisecond},
		{name: "an unset ttl takes the default", want: DefaultReconcileGrace},
		{name: "a negative grace is not unset", ttl: 30 * time.Second, grace: -time.Second, want: -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveReconcileGrace(tc.ttl, tc.grace); got != tc.want {
				t.Fatalf("ResolveReconcileGrace(%s, %s) = %s, want %s", tc.ttl, tc.grace, got, tc.want)
			}
		})
	}
}

// When the grace was derived and the derivation fails the interval relation,
// the error has to say the grace was derived: the operator wrote binding_ttl
// and heartbeat_interval, and an error citing a third key sends them to the
// wrong knob.
func TestCheckReconcileGraceNamesTheDerivationWhenUnset(t *testing.T) {
	_, err := CheckReconcileGrace(16*time.Second, 0, 5*time.Second)
	if err == nil {
		t.Fatal("an 8s derived grace against a 5s interval was accepted")
	}
	for _, want := range []string{
		"scheduler.reconcile_grace is unset and defaults to 8s",
		"half of scheduler.binding_ttl",
		"must be at least 2 heartbeat intervals (10s)",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to contain %q", err, want)
		}
	}

	_, err = CheckReconcileGrace(16*time.Second, 8*time.Second, 5*time.Second)
	if err == nil || strings.Contains(err.Error(), "is unset") {
		t.Fatalf("an explicit grace must be refused as written, got %v", err)
	}
}

func TestLoadAppliesSchedulerReconcileGraceAndHeartbeatIntervalEnv(t *testing.T) {
	t.Setenv("SCHEDULER_RECONCILE_GRACE", "12s")
	t.Setenv("SCHEDULER_HEARTBEAT_INTERVAL", "5s")

	cfg, err := Load("", "scheduler")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Scheduler.ReconcileGrace != 12*time.Second {
		t.Fatalf("reconcile grace = %s, want 12s", cfg.Scheduler.ReconcileGrace)
	}
	if cfg.Scheduler.HeartbeatInterval != 5*time.Second {
		t.Fatalf("heartbeat interval = %s, want 5s", cfg.Scheduler.HeartbeatInterval)
	}
}

func TestLoadRejectsInvalidSchedulerReconcileGraceEnv(t *testing.T) {
	t.Setenv("SCHEDULER_RECONCILE_GRACE", "12")

	_, err := Load("", "scheduler")
	if err == nil || !strings.Contains(err.Error(), "invalid SCHEDULER_RECONCILE_GRACE") {
		t.Fatalf("err = %v, want the env parse refusal", err)
	}
}

func TestLoadRejectsInvalidSchedulerHeartbeatIntervalEnv(t *testing.T) {
	t.Setenv("SCHEDULER_HEARTBEAT_INTERVAL", "five")

	_, err := Load("", "scheduler")
	if err == nil || !strings.Contains(err.Error(), "invalid SCHEDULER_HEARTBEAT_INTERVAL") {
		t.Fatalf("err = %v, want the env parse refusal", err)
	}
}

func TestLoadParsesGatewayPlacementBounds(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"gateway": {
			"max_in_flight_creates": 64,
			"max_schedule_retries": -1
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.MaxInFlightCreates != 64 {
		t.Fatalf("expected max_in_flight_creates 64, got %d", cfg.Gateway.MaxInFlightCreates)
	}
	if cfg.Gateway.MaxScheduleRetries != -1 {
		t.Fatalf("expected max_schedule_retries -1, got %d", cfg.Gateway.MaxScheduleRetries)
	}
}

func TestLoadLeavesGatewayPlacementBoundsUnsetByDefault(t *testing.T) {
	cfg, err := Load("", "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.MaxInFlightCreates != 0 || cfg.Gateway.MaxScheduleRetries != 0 {
		t.Fatalf("expected zero (gateway default) bounds, got creates=%d retries=%d",
			cfg.Gateway.MaxInFlightCreates, cfg.Gateway.MaxScheduleRetries)
	}
}

func TestLoadAppliesGatewayPlacementBoundsEnv(t *testing.T) {
	t.Setenv("GATEWAY_MAX_IN_FLIGHT_CREATES", "128")
	t.Setenv("GATEWAY_MAX_SCHEDULE_RETRIES", "1")

	cfg, err := Load("", "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.MaxInFlightCreates != 128 {
		t.Fatalf("expected max_in_flight_creates 128 from env, got %d", cfg.Gateway.MaxInFlightCreates)
	}
	if cfg.Gateway.MaxScheduleRetries != 1 {
		t.Fatalf("expected max_schedule_retries 1 from env, got %d", cfg.Gateway.MaxScheduleRetries)
	}
}

func TestLoadRejectsInvalidGatewayPlacementBoundsEnv(t *testing.T) {
	t.Setenv("GATEWAY_MAX_SCHEDULE_RETRIES", "two")
	if _, err := Load("", "gateway"); err == nil {
		t.Fatal("expected load to fail for non-integer GATEWAY_MAX_SCHEDULE_RETRIES")
	}
	t.Setenv("GATEWAY_MAX_SCHEDULE_RETRIES", "")
	t.Setenv("GATEWAY_MAX_IN_FLIGHT_CREATES", "many")
	if _, err := Load("", "gateway"); err == nil {
		t.Fatal("expected load to fail for non-integer GATEWAY_MAX_IN_FLIGHT_CREATES")
	}
}

func TestLoadDefaultsGatewayBindingCacheAndAuth(t *testing.T) {
	cfg, err := Load("", "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.BindingCacheSize != DefaultGatewayBindingCacheSize {
		t.Fatalf("binding_cache_size = %d, want the default %d when the key is unset", cfg.Gateway.BindingCacheSize, DefaultGatewayBindingCacheSize)
	}
	if cfg.Gateway.BindingCacheTTL != 0 || cfg.Gateway.BindingCacheNegativeTTL != 0 {
		t.Fatalf("binding cache TTLs = %s/%s, want zero (gateway default)", cfg.Gateway.BindingCacheTTL, cfg.Gateway.BindingCacheNegativeTTL)
	}
	if cfg.Gateway.SchedulerAuthToken != "" {
		t.Fatalf("scheduler_auth_token = %q, want unset", cfg.Gateway.SchedulerAuthToken)
	}
}

// Zero is this key's off switch, so an operator writing 0 must not be handed
// the default back.
func TestLoadParsesGatewayBindingCacheAndAuthKeys(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{
		"gateway": {
			"binding_cache_size": 0,
			"binding_cache_ttl": "5s",
			"binding_cache_negative_ttl": "1s",
			"scheduler_auth_token": "file-token"
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.BindingCacheSize != 0 {
		t.Fatalf("binding_cache_size = %d, want the explicit 0 kept", cfg.Gateway.BindingCacheSize)
	}
	if cfg.Gateway.BindingCacheTTL != 5*time.Second {
		t.Fatalf("binding_cache_ttl = %s, want 5s", cfg.Gateway.BindingCacheTTL)
	}
	if cfg.Gateway.BindingCacheNegativeTTL != time.Second {
		t.Fatalf("binding_cache_negative_ttl = %s, want 1s", cfg.Gateway.BindingCacheNegativeTTL)
	}
	if cfg.Gateway.SchedulerAuthToken != "file-token" {
		t.Fatalf("scheduler_auth_token = %q, want file-token", cfg.Gateway.SchedulerAuthToken)
	}
}

func TestLoadRejectsNumericGatewayBindingCacheDurations(t *testing.T) {
	for _, key := range []string{"binding_cache_ttl", "binding_cache_negative_ttl"} {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "config.json")
		content := `{"gateway": {"` + key + `": 5}}`
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write config file failed: %v", err)
		}
		_, err := Load(path, "gateway")
		if err == nil || !strings.Contains(err.Error(), "gateway."+key) {
			t.Fatalf("err = %v, want a refusal naming gateway.%s", err, key)
		}
	}
}

func TestLoadAppliesGatewayBindingCacheAndAuthEnv(t *testing.T) {
	t.Setenv("GATEWAY_BINDING_CACHE_SIZE", "0")
	t.Setenv("GATEWAY_BINDING_CACHE_TTL", "3s")
	t.Setenv("GATEWAY_BINDING_CACHE_NEGATIVE_TTL", "500ms")
	t.Setenv("GATEWAY_SCHEDULER_AUTH_TOKEN", "env-token")

	cfg, err := Load("", "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.BindingCacheSize != 0 {
		t.Fatalf("binding_cache_size = %d, want 0 from env", cfg.Gateway.BindingCacheSize)
	}
	if cfg.Gateway.BindingCacheTTL != 3*time.Second {
		t.Fatalf("binding_cache_ttl = %s, want 3s from env", cfg.Gateway.BindingCacheTTL)
	}
	if cfg.Gateway.BindingCacheNegativeTTL != 500*time.Millisecond {
		t.Fatalf("binding_cache_negative_ttl = %s, want 500ms from env", cfg.Gateway.BindingCacheNegativeTTL)
	}
	if cfg.Gateway.SchedulerAuthToken != "env-token" {
		t.Fatalf("scheduler_auth_token = %q, want env-token", cfg.Gateway.SchedulerAuthToken)
	}
}

func TestLoadRejectsInvalidGatewayBindingCacheEnv(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"GATEWAY_BINDING_CACHE_SIZE", "lots"},
		{"GATEWAY_BINDING_CACHE_TTL", "2"},
		{"GATEWAY_BINDING_CACHE_NEGATIVE_TTL", "soon"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			_, err := Load("", "gateway")
			if err == nil || !strings.Contains(err.Error(), "invalid "+tc.key) {
				t.Fatalf("err = %v, want the env parse refusal for %s", err, tc.key)
			}
		})
	}
}

// A cached "not found" that outlives a cached binding turns a create's first
// request into a 404, so the relation is refused at load rather than silently
// capped at runtime where nobody would see it.
func TestLoadRejectsGatewayNegativeTTLRelations(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{
			name:    "negative",
			content: `{"gateway": {"binding_cache_negative_ttl": "-1s"}}`,
			want:    "gateway.binding_cache_negative_ttl must not be negative",
		},
		{
			name:    "longer than the positive ttl",
			content: `{"gateway": {"binding_cache_ttl": "1s", "binding_cache_negative_ttl": "2s"}}`,
			want:    "must not exceed gateway.binding_cache_ttl",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "config.json")
			if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
				t.Fatalf("write config file failed: %v", err)
			}
			_, err := Load(path, "gateway")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// The relation is only checkable when both sides are written; a negative TTL
// alone has a documented meaning (the cache is off) and is not an error.
func TestLoadAcceptsNegativeGatewayBindingCacheTTLAsOff(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")
	content := `{"gateway": {"binding_cache_ttl": "-1s", "binding_cache_negative_ttl": "1s"}}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}
	cfg, err := Load(path, "gateway")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Gateway.BindingCacheTTL != -time.Second {
		t.Fatalf("binding_cache_ttl = %s, want -1s kept", cfg.Gateway.BindingCacheTTL)
	}
}
