package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
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

// countingScheduler is a real gRPC scheduler that counts the lookups it served,
// so a balancing test can assert on where calls landed rather than on the
// service-config string that was supposed to send them there.
type countingScheduler struct {
	schedulerv1.UnimplementedSchedulerServer
	mu    sync.Mutex
	calls int
}

func (s *countingScheduler) LookupNode(context.Context, *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"}}, nil
}

func (s *countingScheduler) served() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type schedulerReplica struct {
	server  *grpc.Server
	counter *countingScheduler
	addr    string
}

func startSchedulerReplicas(t *testing.T, count int) []*schedulerReplica {
	t.Helper()

	replicas := make([]*schedulerReplica, 0, count)
	for i := 0; i < count; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		server := grpc.NewServer()
		counter := &countingScheduler{}
		schedulerv1.RegisterSchedulerServer(server, counter)
		go func() { _ = server.Serve(listener) }()
		replica := &schedulerReplica{server: server, counter: counter, addr: listener.Addr().String()}
		t.Cleanup(replica.server.Stop)
		replicas = append(replicas, replica)
	}
	return replicas
}

func replicaAddrs(replicas []*schedulerReplica) string {
	addrs := make([]string, 0, len(replicas))
	for _, replica := range replicas {
		addrs = append(addrs, replica.addr)
	}
	return strings.Join(addrs, ",")
}

func lookupN(t *testing.T, client schedulerv1.SchedulerClient, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})
		cancel()
		if err != nil {
			t.Fatalf("LookupNode %d: %v", i, err)
		}
	}
}

// Without the round-robin service config this fails 6/0/0: grpc.NewClient
// balances with pick_first, so every gateway pins itself to one scheduler and
// scaling the tier out moves no traffic at all.
func TestSchedulerConnRoundRobinsAcrossReplicas(t *testing.T) {
	replicas := startSchedulerReplicas(t, 3)

	conn, err := newSchedulerConn(replicaAddrs(replicas), "")
	if err != nil {
		t.Fatalf("newSchedulerConn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := schedulerv1.NewSchedulerClient(conn)

	// The count is taken once every replica has a ready subchannel, because
	// round_robin spreads only over those.
	waitForReplicasServed(t, client, replicas)
	before := make([]int, len(replicas))
	for i, replica := range replicas {
		before[i] = replica.counter.served()
	}

	lookupN(t, client, 6)
	for i, replica := range replicas {
		if got := replica.counter.served() - before[i]; got != 2 {
			t.Fatalf("replica %d served %d of 6 calls, want 2", i, got)
		}
	}
}

// Killing the replica a gateway is talking to must not fail a call. round_robin
// drops a subchannel that cannot connect; the surviving replicas take the load.
func TestSchedulerConnSurvivesReplicaLoss(t *testing.T) {
	replicas := startSchedulerReplicas(t, 3)

	conn, err := newSchedulerConn(replicaAddrs(replicas), "")
	if err != nil {
		t.Fatalf("newSchedulerConn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := schedulerv1.NewSchedulerClient(conn)

	waitForReplicasServed(t, client, replicas)

	replicas[1].server.Stop()
	// The balancer notices the dead subchannel on its next attempt, so one
	// failure is tolerated here; what must not happen is calls failing after it.
	_, _ = client.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})

	before := []int{replicas[0].counter.served(), replicas[2].counter.served()}
	lookupN(t, client, 6)
	survived := (replicas[0].counter.served() - before[0]) + (replicas[2].counter.served() - before[1])
	if survived != 6 {
		t.Fatalf("surviving replicas served %d of 6 calls after a replica was killed", survived)
	}
}

// A single address keeps dialling exactly as it did before the list form
// existed, which is what every shipped config still writes.
func TestSchedulerDialTargetKeepsASingleAddressUnchanged(t *testing.T) {
	target, options, err := schedulerDialTarget(" scheduler:9090 ")
	if err != nil {
		t.Fatalf("schedulerDialTarget: %v", err)
	}
	if target != "scheduler:9090" || len(options) != 0 {
		t.Fatalf("schedulerDialTarget() = %q with %d options, want the bare address and no resolver", target, len(options))
	}

	if _, _, err := schedulerDialTarget(" , "); err == nil {
		t.Fatal("schedulerDialTarget accepted an address naming no host")
	}
}

// waitForReplicasServed drives calls until every replica has answered one.
// round_robin spreads over the subchannels that are ready, and they become
// ready as their connections come up, so a balancing assertion is only
// meaningful once they all have.
func waitForReplicasServed(t *testing.T, client schedulerv1.SchedulerClient, replicas []*schedulerReplica) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		served := 0
		for _, replica := range replicas {
			if replica.counter.served() > 0 {
				served++
			}
		}
		if served == len(replicas) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d replicas ever served a call", served, len(replicas))
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _ = client.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})
		cancel()
		time.Sleep(20 * time.Millisecond)
	}
}
