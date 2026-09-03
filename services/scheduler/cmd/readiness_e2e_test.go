package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
)

// The readiness split has to hold against the real binary, not just the gate.
//
// `newReadinessGate` publishes fitness only on the named gRPC service and pins
// the overall service SERVING, so that liveness never restarts a tier for one
// Redis outage while readiness still removes the pod. The shipped manifest
// probed without `-service` for the whole life of the feature, which read the
// always-SERVING overall status: every pod was Ready from process start and the
// gate did nothing. Unit tests could not see it because they assert the split in
// isolation, and nothing connected the split to what a probe actually asks.
//
// Driven by pointing the store at a closed port: the probe fails, and after
// three consecutive failures the named service must go NOT_SERVING while the
// overall one stays SERVING. That difference is the entire feature.
func TestReadinessGateIsVisibleToAServiceScopedProbe(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the scheduler binary")
	}

	binary := filepath.Join(t.TempDir(), "scheduler")
	build := exec.Command("go", "build", "-o", binary, "./scheduler/cmd")
	build.Dir = servicesRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build scheduler: %v\n%s", err, out)
	}

	// A real Redis, killed mid-life. The scheduler refuses to boot at all when
	// its store is unreachable at startup, so the outage this gate exists for is
	// the one that arrives after a healthy connection -- which is also the one a
	// crash loop would not fix.
	redisBin := os.Getenv("REDIS_SERVER_BIN")
	if redisBin == "" {
		if found, err := exec.LookPath("redis-server"); err == nil {
			redisBin = found
		} else {
			t.Skip("redis-server not found; set REDIS_SERVER_BIN")
		}
	}
	redisAddr := reserveLocalAddr(t)
	_, redisPort, err := net.SplitHostPort(redisAddr)
	if err != nil {
		t.Fatalf("split redis addr: %v", err)
	}
	redis := exec.Command(redisBin, "--port", redisPort, "--bind", "127.0.0.1", "--save", "")
	if err := redis.Start(); err != nil {
		t.Fatalf("start redis: %v", err)
	}
	redisStopped := false
	t.Cleanup(func() {
		if !redisStopped {
			_ = redis.Process.Kill()
		}
		_ = redis.Wait()
	})
	waitForListener(t, redisAddr, 15*time.Second)

	grpcAddr := reserveLocalAddr(t)
	configPath := filepath.Join(t.TempDir(), "scheduler.json")
	config := map[string]any{
		"log_level": "error",
		"scheduler": map[string]any{
			"grpc_listen_addr":     grpcAddr,
			"metrics_listen_addr":  reserveLocalAddr(t),
			"redis_addr":           redisAddr,
			"store_probe_interval": "200ms",
		},
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("encode config: %v", err)
	}
	if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	server := exec.CommandContext(ctx, binary, "-config", configPath)
	server.Stdout, server.Stderr = os.Stderr, os.Stderr
	if err := server.Start(); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}
	t.Cleanup(func() {
		_ = server.Process.Kill()
		_ = server.Wait()
	})
	waitForListener(t, grpcAddr, 30*time.Second)

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial scheduler: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	health := grpc_health_v1.NewHealthClient(conn)

	check := func(service string) grpc_health_v1.HealthCheckResponse_ServingStatus {
		callCtx, callCancel := context.WithTimeout(ctx, 2*time.Second)
		defer callCancel()
		resp, err := health.Check(callCtx, &grpc_health_v1.HealthCheckRequest{Service: service})
		if err != nil {
			return grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN
		}
		return resp.GetStatus()
	}
	awaitStatus := func(service string, want grpc_health_v1.HealthCheckResponse_ServingStatus, within time.Duration) grpc_health_v1.HealthCheckResponse_ServingStatus {
		deadline := time.Now().Add(within)
		var last grpc_health_v1.HealthCheckResponse_ServingStatus
		for time.Now().Before(deadline) {
			last = check(service)
			if last == want {
				return last
			}
			time.Sleep(200 * time.Millisecond)
		}
		return last
	}

	if got := awaitStatus("scheduler.v1.Scheduler", grpc_health_v1.HealthCheckResponse_SERVING, 30*time.Second); got != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("a healthy scheduler never reported its named service SERVING (last %v)", got)
	}

	// Now take the store away.
	redisStopped = true
	if err := redis.Process.Kill(); err != nil {
		t.Fatalf("kill redis: %v", err)
	}
	_ = redis.Wait()

	// The named service must go NOT_SERVING once the store probe gives up.
	named := awaitStatus("scheduler.v1.Scheduler", grpc_health_v1.HealthCheckResponse_NOT_SERVING, 30*time.Second)
	if named != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("scheduler.v1.Scheduler is %v with an unreachable binding store; a readiness "+
			"probe scoped to it would keep the replica in the Service", named)
	}

	// ...while the overall service stays SERVING, or liveness would restart the
	// whole tier for the same outage. This is also the status a probe with no
	// -service reads, which is why omitting the flag made the gate inert.
	if overall := check(""); overall != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("the overall health service is %v; liveness reads this one and would restart "+
			"every replica for one store outage", overall)
	}
}

// servicesRoot returns the Go module root, so `go build ./scheduler/cmd` works
// regardless of which package directory the test binary runs from.
func servicesRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test's working directory")
		}
		dir = parent
	}
}

// waitForListener blocks until something accepts on addr.
func waitForListener(t *testing.T, addr string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("nothing listening on %s after %s", addr, within)
}

// reserveLocalAddr returns a loopback address whose port was free, then closes
// it. Nothing listens there afterwards, which is what the dead-store case needs.
func reserveLocalAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return fmt.Sprintf("127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port)
}
