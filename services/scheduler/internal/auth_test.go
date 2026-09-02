package scheduler

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testAuthToken = "s3cr3t-scheduler-token"

func unaryInfo(method string) *grpc.UnaryServerInfo {
	return &grpc.UnaryServerInfo{FullMethod: method}
}

func passHandler(_ context.Context, _ any) (any, error) { return "ok", nil }

func withBearer(ctx context.Context, header string) context.Context {
	return metadata.NewIncomingContext(ctx, metadata.Pairs(AuthMetadataKey, header))
}

// With no token configured every RPC passes, metadata or not. This is the
// shipped default and the rollout shape: the scheduler is upgraded first, the
// secret distributed second, enforcement switched on last.
func TestAuthUnsetAcceptsEverything(t *testing.T) {
	interceptor := AuthUnaryInterceptor(nil)
	for name, ctx := range map[string]context.Context{
		"no metadata":  context.Background(),
		"wrong token":  withBearer(context.Background(), "Bearer nope"),
		"garbage":      withBearer(context.Background(), "not-a-bearer"),
		"right anyway": withBearer(context.Background(), "Bearer "+testAuthToken),
	} {
		if _, err := interceptor(ctx, nil, unaryInfo("/agentenv.scheduler.v1.Scheduler/Heartbeat"), passHandler); err != nil {
			t.Fatalf("%s: unset token refused an RPC: %v", name, err)
		}
	}
}

func TestAuthSetRefusesMissingMalformedAndWrongTokens(t *testing.T) {
	interceptor := AuthUnaryInterceptor([]byte(testAuthToken))
	method := "/agentenv.scheduler.v1.Scheduler/RecordAssignment"

	for _, tc := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "no metadata", ctx: context.Background()},
		{name: "no authorization key", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-other", "v"))},
		{name: "empty value", ctx: withBearer(context.Background(), "")},
		{name: "no scheme", ctx: withBearer(context.Background(), testAuthToken)},
		{name: "wrong scheme", ctx: withBearer(context.Background(), "Basic "+testAuthToken)},
		{name: "scheme only", ctx: withBearer(context.Background(), "Bearer ")},
		{name: "wrong token", ctx: withBearer(context.Background(), "Bearer "+testAuthToken+"x")},
		{name: "prefix of the token", ctx: withBearer(context.Background(), "Bearer "+testAuthToken[:len(testAuthToken)-1])},
		{
			name: "two credentials",
			ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs(
				AuthMetadataKey, "Bearer wrong",
				AuthMetadataKey, "Bearer "+testAuthToken,
			)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := interceptor(tc.ctx, nil, unaryInfo(method), passHandler)
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("err = %v, want Unauthenticated", err)
			}
			if resp != nil {
				t.Fatalf("a refused RPC reached the handler: %v", resp)
			}
		})
	}
}

func TestAuthSetAcceptsTheTokenAndExemptsHealth(t *testing.T) {
	interceptor := AuthUnaryInterceptor([]byte(testAuthToken))

	for _, header := range []string{
		"Bearer " + testAuthToken,
		"bearer " + testAuthToken,
		"BEARER " + testAuthToken,
		"  Bearer   " + testAuthToken + "  ",
	} {
		if _, err := interceptor(withBearer(context.Background(), header), nil,
			unaryInfo("/agentenv.scheduler.v1.Scheduler/Schedule"), passHandler); err != nil {
			t.Fatalf("header %q refused: %v", header, err)
		}
	}

	// Kubernetes probes with grpc_health_probe, which carries no token.
	if _, err := interceptor(context.Background(), nil, unaryInfo("/grpc.health.v1.Health/Check"), passHandler); err != nil {
		t.Fatalf("health check refused without a token: %v", err)
	}
}

// The counter is the operator's way of telling an unconfigured client from an
// attacker; each refusal lands under exactly one reason.
func TestAuthRejectionsAreCountedByReason(t *testing.T) {
	interceptor := AuthUnaryInterceptor([]byte(testAuthToken))
	method := "/agentenv.scheduler.v1.Scheduler/UnregisterNode"
	before := map[string]float64{}
	for _, reason := range []string{authRejectMissing, authRejectMalformed, authRejectInvalid} {
		before[reason] = counterValue(t, schedulerAuthRejectedTotal.WithLabelValues(reason))
	}

	_, _ = interceptor(context.Background(), nil, unaryInfo(method), passHandler)
	_, _ = interceptor(withBearer(context.Background(), "Basic x"), nil, unaryInfo(method), passHandler)
	_, _ = interceptor(withBearer(context.Background(), "Bearer wrong"), nil, unaryInfo(method), passHandler)

	for _, reason := range []string{authRejectMissing, authRejectMalformed, authRejectInvalid} {
		delta := counterValue(t, schedulerAuthRejectedTotal.WithLabelValues(reason)) - before[reason]
		if delta != 1 {
			t.Fatalf("reason %q counted %v refusals, want 1", reason, delta)
		}
	}
}

func TestResolveAuthTokenSources(t *testing.T) {
	dir := t.TempDir()
	write := func(name, contents string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return path
	}

	if token, err := ResolveAuthToken("", ""); err != nil || token != nil {
		t.Fatalf("unset must resolve to nil, got %q, %v", token, err)
	}
	if token, err := ResolveAuthToken("  literal  ", ""); err != nil || string(token) != "literal" {
		t.Fatalf("literal token = %q, %v", token, err)
	}
	if token, err := ResolveAuthToken("", write("token", "from-file\n")); err != nil || string(token) != "from-file" {
		t.Fatalf("file token = %q, %v", token, err)
	}

	// The operator pointed at a file and meant enforcement; nothing here may
	// quietly resolve to "auth off".
	if _, err := ResolveAuthToken("", write("empty", "\n")); err == nil {
		t.Fatal("an empty token file must be an error, not an open listener")
	}
	if _, err := ResolveAuthToken("", filepath.Join(dir, "missing")); err == nil {
		t.Fatal("a missing token file must be an error, not an open listener")
	}
	if _, err := ResolveAuthToken("literal", write("both", "x")); err == nil {
		t.Fatal("configuring both a literal and a file must be refused")
	}
	if _, err := ResolveAuthToken("", write("huge", strings.Repeat("a", maxAuthTokenLen+1))); err == nil {
		t.Fatal("a token past the size bound must be refused")
	}
}

// authenticatedServer serves the real Service over an in-memory listener with
// the interceptor chain main.go installs, so the test exercises the same path
// a node or gateway does rather than the interceptor in isolation.
func authenticatedServer(t *testing.T, token []byte) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(MetricsUnaryInterceptor(), AuthUnaryInterceptor(token)),
		grpc.ChainStreamInterceptor(AuthStreamInterceptor(token)),
	)
	service := NewService(nil,
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Minute),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(time.Minute),
	)
	schedulerv1.RegisterSchedulerServer(server, service)
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(server, hs)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// Every RPC the scheduler serves — placement, bindings, heartbeats, P2P index
// and the mobility records — refuses a caller without the token, and the
// health service does not. Iterating the service descriptor rather than a
// hand-written list is what keeps a future RPC from shipping unauthenticated.
func TestEveryServiceRPCRequiresTheTokenWhenSet(t *testing.T) {
	conn := authenticatedServer(t, []byte(testAuthToken))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	methods := schedulerv1.Scheduler_ServiceDesc.Methods
	if len(methods) < 18 {
		t.Fatalf("service descriptor lists %d unary methods; expected the full scheduler surface", len(methods))
	}
	for _, method := range methods {
		fullMethod := "/" + schedulerv1.Scheduler_ServiceDesc.ServiceName + "/" + method.MethodName
		t.Run(method.MethodName, func(t *testing.T) {
			// An empty message decodes into every request type, so the
			// call reaches the interceptor whatever the RPC expects.
			err := conn.Invoke(ctx, fullMethod, &schedulerv1.ListNodesRequest{}, &schedulerv1.ListNodesResponse{})
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("without a token: err = %v, want Unauthenticated", err)
			}

			authed := metadata.AppendToOutgoingContext(ctx, AuthMetadataKey, AuthBearerScheme+" "+testAuthToken)
			err = conn.Invoke(authed, fullMethod, &schedulerv1.ListNodesRequest{}, &schedulerv1.ListNodesResponse{})
			if status.Code(err) == codes.Unauthenticated {
				t.Fatalf("with the token: still Unauthenticated: %v", err)
			}
		})
	}

	// The probe path stays open or every replica goes unready when the token
	// is switched on.
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check without a token: %v", err)
	}
}

// The same server with no token configured accepts the same calls, which is
// what a mixed-version fleet relies on during the rollout.
func TestServerWithoutATokenAcceptsUnauthenticatedCalls(t *testing.T) {
	conn := authenticatedServer(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := schedulerv1.NewSchedulerClient(conn).ListNodes(ctx, &schedulerv1.ListNodesRequest{})
	if err != nil {
		t.Fatalf("ListNodes without a token on an open server: %v", err)
	}
	if len(resp.GetNodes()) != 1 {
		t.Fatalf("nodes = %v, want the one configured node", resp.GetNodes())
	}
}
