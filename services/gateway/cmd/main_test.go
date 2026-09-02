package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	gateway "agentenv/services/gateway/internal"
	"agentenv/services/shared/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const testAPIKey = "e2b_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestValidateAPIKey(t *testing.T) {
	t.Parallel()

	got, err := validateAPIKey(testAPIKey, "test")
	if err != nil {
		t.Fatalf("validateAPIKey() error = %v", err)
	}
	if got != testAPIKey {
		t.Fatalf("validateAPIKey() = %q, want %q", got, testAPIKey)
	}

	for _, invalid := range []string{"", "too-short", " " + testAPIKey, testAPIKey + "\n", strings.Repeat("a", 31), strings.Repeat("a", maxAPIKeyLen+1), strings.Repeat("a", 31) + "!"} {
		if _, err := validateAPIKey(invalid, "test"); err == nil {
			t.Errorf("validateAPIKey(%q) unexpectedly succeeded", invalid)
		}
	}
}

func TestLoadAPIKeyFromEnvironment(t *testing.T) {
	got, err := loadAPIKeyFrom(
		func(name string) (string, bool) { return testAPIKey, name == apiKeyEnv },
		filepath.Join(t.TempDir(), "missing"),
	)
	if err != nil {
		t.Fatalf("loadAPIKey() error = %v", err)
	}
	if got != testAPIKey {
		t.Fatalf("loadAPIKey() = %q, want %q", got, testAPIKey)
	}
}

func TestLoadAPIKeyRejectsExplicitEmptyEnvironment(t *testing.T) {
	if _, err := loadAPIKeyFrom(
		func(name string) (string, bool) { return "", name == apiKeyEnv },
		filepath.Join(t.TempDir(), "missing"),
	); err == nil {
		t.Fatal("loadAPIKey() unexpectedly accepted an empty environment value")
	}
}

func TestLoadAPIKeyFromFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "api-key")
	if err := os.WriteFile(path, []byte(testAPIKey+"\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	got, err := loadAPIKeyFrom(func(string) (string, bool) { return "", false }, path)
	if err != nil {
		t.Fatalf("loadAPIKeyFrom() error = %v", err)
	}
	if got != testAPIKey {
		t.Fatalf("loadAPIKeyFrom() = %q, want %q", got, testAPIKey)
	}
}

func TestLoadAPIKeyRejectsMissingFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	missing := filepath.Join(dir, "missing")
	if _, err := loadAPIKeyFrom(func(string) (string, bool) { return "", false }, missing); err == nil {
		t.Fatal("loadAPIKeyFrom() unexpectedly accepted a missing secret")
	}
}

func TestLoadAPIKeyRejectsNonRegularFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "api-key")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAPIKeyFrom(func(string) (string, bool) { return "", false }, path); err == nil {
		t.Fatal("loadAPIKeyFrom() unexpectedly accepted a FIFO")
	}
}

func TestLoadAPIKeyAllowsSymlinkedSecret(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "..data-api-key")
	path := filepath.Join(dir, "api-key")
	if err := os.WriteFile(target, []byte(testAPIKey+"\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(target), path); err != nil {
		t.Fatal(err)
	}
	got, err := loadAPIKeyFrom(func(string) (string, bool) { return "", false }, path)
	if err != nil {
		t.Fatalf("loadAPIKeyFrom() error = %v", err)
	}
	if got != testAPIKey {
		t.Fatalf("loadAPIKeyFrom() = %q, want %q", got, testAPIKey)
	}
}

// A key that parses and is documented but never reaches the server is a
// setting that silently does nothing. Each server-facing gateway key is
// threaded from the loaded config into the options the server is built with;
// the binding cache's size is the one an earlier round shipped unpinned.
func TestServerOptionsThreadEveryGatewayKey(t *testing.T) {
	t.Parallel()

	cfg := config.GatewayConfig{
		RequestTimeout:          7 * time.Second,
		ForwardResponseSize:     1234,
		DebugMode:               true,
		SandboxProxyDomains:     []string{"sbx.example"},
		MaxIdleConnsPerHost:     9,
		BindingCacheSize:        4321,
		BindingCacheTTL:         3 * time.Second,
		BindingCacheNegativeTTL: 150 * time.Millisecond,
		MaxInFlightCreates:      77,
		MaxScheduleRetries:      5,
	}
	queryOnly := schedulerv1.NewSchedulerClient(nil)

	got := serverOptionsFromConfig(cfg, testAPIKey, queryOnly)
	want := gateway.ServerOptions{
		APIKey:                   testAPIKey,
		RequestTimeout:           7 * time.Second,
		MaxResponseSize:          1234,
		DebugMode:                true,
		SandboxProxyDomains:      []string{"sbx.example"},
		QueryOnlySchedulerClient: queryOnly,
		MaxIdleConnsPerHost:      9,
		BindingCache: gateway.BindingCacheOptions{
			Size:        4321,
			TTL:         3 * time.Second,
			NegativeTTL: 150 * time.Millisecond,
		},
		MaxInFlightCreates: 77,
		MaxScheduleRetries: 5,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("serverOptionsFromConfig() = %+v, want %+v", got, want)
	}
}

// tokenRecordingScheduler is a real gRPC scheduler that keeps the metadata each
// LookupNode arrived with. The credential is only observable on the wire, and
// only newSchedulerConn decides whether it is there, so the binary's own dial
// path is what is exercised.
type tokenRecordingScheduler struct {
	schedulerv1.UnimplementedSchedulerServer
	seen chan metadata.MD
}

func (s *tokenRecordingScheduler) LookupNode(ctx context.Context, _ *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.seen <- md
	return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"}}, nil
}

// The scheduler's interceptor reads one key in one shape. The gateway dials
// with the configured token on every scheduler connection, and with nothing
// when no token is configured — which is how it keeps talking to a scheduler
// that does not enforce one yet.
func TestSchedulerConnCarriesTheConfiguredToken(t *testing.T) {
	for _, tc := range []struct {
		name  string
		token string
		want  []string
	}{
		{name: "a configured token is presented as Bearer", token: "shared-secret", want: []string{"Bearer shared-secret"}},
		{name: "no token dials without a credential", token: "", want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			server := grpc.NewServer()
			recorder := &tokenRecordingScheduler{seen: make(chan metadata.MD, 1)}
			schedulerv1.RegisterSchedulerServer(server, recorder)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)

			conn, err := newSchedulerConn(listener.Addr().String(), tc.token)
			if err != nil {
				t.Fatalf("newSchedulerConn: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close() })

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := schedulerv1.NewSchedulerClient(conn).LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"}); err != nil {
				t.Fatalf("LookupNode: %v", err)
			}

			var md metadata.MD
			select {
			case md = <-recorder.seen:
			case <-time.After(5 * time.Second):
				t.Fatal("scheduler saw no call")
			}
			if got := md.Get("authorization"); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("authorization = %v, want %v", got, tc.want)
			}
		})
	}
}
