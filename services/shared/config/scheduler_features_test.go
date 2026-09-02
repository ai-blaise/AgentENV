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
