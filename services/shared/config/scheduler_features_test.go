package config

import (
	"strings"
	"testing"
	"time"
)

// Routing must outlive placement eligibility: a node's bindings expiring while
// it is still a placement candidate 404s live sandboxes on a node the
// scheduler is simultaneously filling. The relation holds unconditionally,
// not only when a heartbeat interval is configured.
func TestValidateReportTTLDoesNotExceedBindingTTL(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reportTTL  time.Duration
		bindingTTL time.Duration
		wantErr    bool
	}{
		{name: "equal, the shipped defaults", reportTTL: 30 * time.Second, bindingTTL: 30 * time.Second},
		{name: "bindings outlive placement", reportTTL: 20 * time.Second, bindingTTL: 60 * time.Second},
		{name: "bindings expire before placement stops", reportTTL: 60 * time.Second, bindingTTL: 30 * time.Second, wantErr: true},
		{name: "off by one second", reportTTL: 31 * time.Second, bindingTTL: 30 * time.Second, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSchedulerTTLOrdering(SchedulerConfig{ReportTTL: tc.reportTTL, BindingTTL: tc.bindingTTL})
			if tc.wantErr && err == nil {
				t.Fatal("expected a validation error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}

	// And through Load, so it is a startup refusal rather than a helper's opinion.
	path := writeSchedulerConfig(t, `"report_ttl": "45s", "binding_ttl": "30s"`)
	if _, err := Load(path, "scheduler"); err == nil || !strings.Contains(err.Error(), "must not exceed scheduler.binding_ttl") {
		t.Fatalf("Load err = %v, want the report/binding ordering refusal", err)
	}
}

func TestSchedulerAuthTokenKeys(t *testing.T) {
	path := writeSchedulerConfig(t, `"auth_token": "from-config"`)
	cfg, err := Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Scheduler.AuthToken != "from-config" || cfg.Scheduler.AuthTokenFile != "" {
		t.Fatalf("auth_token = %q file = %q", cfg.Scheduler.AuthToken, cfg.Scheduler.AuthTokenFile)
	}

	t.Setenv("SCHEDULER_AUTH_TOKEN", "from-env")
	cfg, err = Load(path, "scheduler")
	if err != nil {
		t.Fatalf("load with env: %v", err)
	}
	if cfg.Scheduler.AuthToken != "from-env" {
		t.Fatalf("SCHEDULER_AUTH_TOKEN did not override: %q", cfg.Scheduler.AuthToken)
	}

	// Both a literal and a file is ambiguous and refused at load.
	t.Setenv("SCHEDULER_AUTH_TOKEN_FILE", "/run/secrets/scheduler-token")
	if _, err := Load(path, "scheduler"); err == nil || !strings.Contains(err.Error(), "both set") {
		t.Fatalf("Load err = %v, want a refusal of token plus token file", err)
	}

	t.Setenv("SCHEDULER_AUTH_TOKEN", "")
	cfg, err = Load(writeSchedulerConfig(t, `"strategy": "round_robin"`), "scheduler")
	if err != nil {
		t.Fatalf("load with file only: %v", err)
	}
	if cfg.Scheduler.AuthTokenFile != "/run/secrets/scheduler-token" || cfg.Scheduler.AuthToken != "" {
		t.Fatalf("SCHEDULER_AUTH_TOKEN_FILE did not apply: %q / %q", cfg.Scheduler.AuthTokenFile, cfg.Scheduler.AuthToken)
	}

	// Unset is the default and means auth off.
	t.Setenv("SCHEDULER_AUTH_TOKEN_FILE", "")
	cfg, err = Load(writeSchedulerConfig(t, `"strategy": "round_robin"`), "scheduler")
	if err != nil {
		t.Fatalf("load default: %v", err)
	}
	if cfg.Scheduler.AuthToken != "" || cfg.Scheduler.AuthTokenFile != "" {
		t.Fatal("auth must default to unset")
	}
}

// The ledger ships off and its clamp ships at 512; both are reachable from the
// file and the environment, and the clamp cannot be negative.
func TestSchedulerReservationKeys(t *testing.T) {
	cfg, err := Load(writeSchedulerConfig(t, `"strategy": "round_robin"`), "scheduler")
	if err != nil {
		t.Fatalf("load default: %v", err)
	}
	if cfg.Scheduler.ReservationsEnabled {
		t.Fatal("reservations must ship disabled")
	}
	if cfg.Scheduler.MaxReservationDelta != 512 {
		t.Fatalf("default max_reservation_delta = %d, want 512", cfg.Scheduler.MaxReservationDelta)
	}

	cfg, err = Load(writeSchedulerConfig(t, `"reservations_enabled": true, "max_reservation_delta": 64`), "scheduler")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !cfg.Scheduler.ReservationsEnabled || cfg.Scheduler.MaxReservationDelta != 64 {
		t.Fatalf("parsed enabled=%v delta=%d", cfg.Scheduler.ReservationsEnabled, cfg.Scheduler.MaxReservationDelta)
	}

	t.Setenv("SCHEDULER_RESERVATIONS_ENABLED", "false")
	t.Setenv("SCHEDULER_MAX_RESERVATION_DELTA", "7")
	cfg, err = Load(writeSchedulerConfig(t, `"reservations_enabled": true, "max_reservation_delta": 64`), "scheduler")
	if err != nil {
		t.Fatalf("load with env: %v", err)
	}
	if cfg.Scheduler.ReservationsEnabled || cfg.Scheduler.MaxReservationDelta != 7 {
		t.Fatalf("env did not override: enabled=%v delta=%d", cfg.Scheduler.ReservationsEnabled, cfg.Scheduler.MaxReservationDelta)
	}

	t.Setenv("SCHEDULER_MAX_RESERVATION_DELTA", "-1")
	if _, err := Load(writeSchedulerConfig(t, `"strategy": "round_robin"`), "scheduler"); err == nil {
		t.Fatal("a negative clamp must be refused")
	}
	t.Setenv("SCHEDULER_MAX_RESERVATION_DELTA", "many")
	if _, err := Load(writeSchedulerConfig(t, `"strategy": "round_robin"`), "scheduler"); err == nil {
		t.Fatal("a non-integer clamp must be refused")
	}
	t.Setenv("SCHEDULER_MAX_RESERVATION_DELTA", "")
	t.Setenv("SCHEDULER_RESERVATIONS_ENABLED", "sometimes")
	if _, err := Load(writeSchedulerConfig(t, `"strategy": "round_robin"`), "scheduler"); err == nil {
		t.Fatal("a non-boolean switch must be refused")
	}
}

// The P2P lookup limit ships at 8, not unlimited; an explicit non-positive
// value still means unlimited for an operator who wants that.
func TestSchedulerArtifactLookupNodeLimitDefaultsToEight(t *testing.T) {
	cfg, err := Load(writeSchedulerConfig(t, `"strategy": "round_robin"`), "scheduler")
	if err != nil {
		t.Fatalf("load default: %v", err)
	}
	if cfg.Scheduler.ArtifactLookupNodeLimit != 8 {
		t.Fatalf("default artifact_lookup_node_limit = %d, want 8", cfg.Scheduler.ArtifactLookupNodeLimit)
	}
}

// The mode decides one thing that cannot be recovered from at runtime: whether
// this process may run without a shared binding store. Three replicas with
// three private binding maps answer a routing lookup correctly one time in
// three, and nothing says so until a client is told a running sandbox does not
// exist.
func TestSchedulerReplicaRequiresASharedStore(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      SchedulerMode
		redisAddr string
		wantErr   string
	}{
		{name: "a replica without redis is refused", mode: SchedulerModeReplica, wantErr: "requires scheduler.redis_addr"},
		{name: "a replica with redis starts", mode: SchedulerModeReplica, redisAddr: "redis:6379"},
		{name: "query-only without redis is refused", mode: SchedulerModeQueryOnly, wantErr: "requires scheduler.redis_addr"},
		{name: "a primary without redis still starts", mode: SchedulerModePrimary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `"strategy": "round_robin"`
			if tc.redisAddr != "" {
				body += `, "redis_addr": "` + tc.redisAddr + `"`
			}
			_, err := LoadScheduler(writeSchedulerConfig(t, body), tc.mode)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("LoadScheduler(%s) failed: %v", tc.mode, err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("LoadScheduler(%s) accepted a config it should refuse", tc.mode)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("LoadScheduler(%s) error = %v, want it to name %q", tc.mode, err, tc.wantErr)
			}
		})
	}
}

// --query-only is what every shipped manifest and runbook says today, so it
// keeps working. The two spellings can disagree, so setting both is refused
// rather than resolved by precedence.
func TestResolveSchedulerMode(t *testing.T) {
	env := func(pairs map[string]string) func(string) (string, bool) {
		return func(key string) (string, bool) {
			value, ok := pairs[key]
			return value, ok
		}
	}

	for _, tc := range []struct {
		name      string
		modeFlag  string
		queryOnly bool
		env       map[string]string
		want      SchedulerMode
		wantErr   bool
	}{
		{name: "nothing set is a primary", want: SchedulerModePrimary},
		{name: "the flag wins", modeFlag: "replica", env: map[string]string{"SCHEDULER_MODE": "primary"}, want: SchedulerModeReplica},
		{name: "the environment is read when the flag is not set", env: map[string]string{"SCHEDULER_MODE": "query-only"}, want: SchedulerModeQueryOnly},
		{name: "the deprecated alias still selects query-only", queryOnly: true, want: SchedulerModeQueryOnly},
		{name: "both spellings at once is refused", modeFlag: "replica", queryOnly: true, wantErr: true},
		{name: "an unknown mode is refused", modeFlag: "leader", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveSchedulerMode(tc.modeFlag, tc.queryOnly, env(tc.env))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ResolveSchedulerMode() = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveSchedulerMode() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("ResolveSchedulerMode() = %q, want %q", got, tc.want)
			}
		})
	}
}

// SchedulerConfig.UnmarshalJSON whitelists every field it reads, so a key added
// to the struct and forgotten there parses into nothing and the operator's
// value is silently the default.
func TestSchedulerNodeStreamKeysAreParsed(t *testing.T) {
	path := writeSchedulerConfig(t, `"redis_addr": "redis:6379",
		"node_stream_enabled": false,
		"node_stream_maxlen": 4096,
		"node_stream_publish_queue": 128,
		"node_stream_warmup_timeout": "3s",
		"store_probe_interval": "500ms"`)

	cfg, err := LoadScheduler(path, SchedulerModeReplica)
	if err != nil {
		t.Fatalf("LoadScheduler: %v", err)
	}
	if cfg.Scheduler.NodeStreamEnabledFor(SchedulerModeReplica) {
		t.Fatal("node_stream_enabled = false did not disable the stream for a replica")
	}
	if cfg.Scheduler.NodeStreamMaxLen != 4096 {
		t.Fatalf("node_stream_maxlen = %d, want 4096", cfg.Scheduler.NodeStreamMaxLen)
	}
	if cfg.Scheduler.NodeStreamPublishQueue != 128 {
		t.Fatalf("node_stream_publish_queue = %d, want 128", cfg.Scheduler.NodeStreamPublishQueue)
	}
	if cfg.Scheduler.NodeStreamWarmupTimeout != 3*time.Second {
		t.Fatalf("node_stream_warmup_timeout = %s, want 3s", cfg.Scheduler.NodeStreamWarmupTimeout)
	}
	if cfg.Scheduler.StoreProbeInterval != 500*time.Millisecond {
		t.Fatalf("store_probe_interval = %s, want 500ms", cfg.Scheduler.StoreProbeInterval)
	}
}

// The default is on for a replica, because a replica that does not replicate is
// strictly worse than the single scheduler it was scaled out from, and off
// everywhere else, because nothing else has peers to hear from.
func TestSchedulerNodeStreamDefaultsByMode(t *testing.T) {
	cfg, err := LoadScheduler(writeSchedulerConfig(t, `"redis_addr": "redis:6379"`), SchedulerModeReplica)
	if err != nil {
		t.Fatalf("LoadScheduler: %v", err)
	}
	for _, tc := range []struct {
		mode SchedulerMode
		want bool
	}{
		{mode: SchedulerModeReplica, want: true},
		{mode: SchedulerModePrimary, want: false},
		{mode: SchedulerModeQueryOnly, want: false},
	} {
		if got := cfg.Scheduler.NodeStreamEnabledFor(tc.mode); got != tc.want {
			t.Fatalf("NodeStreamEnabledFor(%s) = %v, want %v", tc.mode, got, tc.want)
		}
	}
}
