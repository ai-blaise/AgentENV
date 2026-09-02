package gateway

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// metadataRecordingScheduler is a real gRPC scheduler that answers LookupNode
// and keeps the metadata each call arrived with. The credential is only
// observable on the wire, so a stub client cannot pin it.
type metadataRecordingScheduler struct {
	schedulerv1.UnimplementedSchedulerServer
	seen chan metadata.MD
}

func (s *metadataRecordingScheduler) LookupNode(ctx context.Context, _ *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.seen <- md
	return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"}}, nil
}

// startRecordingScheduler serves on a loopback port and returns its address
// and the channel the incoming metadata lands on.
func startRecordingScheduler(t *testing.T) (string, <-chan metadata.MD) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	recorder := &metadataRecordingScheduler{seen: make(chan metadata.MD, 8)}
	schedulerv1.RegisterSchedulerServer(server, recorder)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String(), recorder.seen
}

// dialScheduler dials exactly as the gateway binary does: insecure transport
// plus whatever SchedulerDialOptions adds for the token.
func dialScheduler(t *testing.T, addr string, token string) schedulerv1.SchedulerClient {
	t.Helper()
	options := append(
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		SchedulerDialOptions(token)...,
	)
	conn, err := grpc.NewClient(addr, options...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return schedulerv1.NewSchedulerClient(conn)
}

func receiveMetadata(t *testing.T, seen <-chan metadata.MD) metadata.MD {
	t.Helper()
	select {
	case md := <-seen:
		return md
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler saw no call")
		return nil
	}
}

// The scheduler's interceptor reads exactly one key in exactly one shape. A
// different key, a missing scheme, or a changed scheme is an unauthenticated
// gateway the moment enforcement lands, and nothing before then would notice.
func TestSchedulerRPCsCarryTheBearerToken(t *testing.T) {
	addr, seen := startRecordingScheduler(t)
	client := dialScheduler(t, addr, "s3cret-token")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"}); err != nil {
		t.Fatalf("LookupNode: %v", err)
	}

	md := receiveMetadata(t, seen)
	values := md.Get("authorization")
	if len(values) != 1 {
		t.Fatalf("authorization metadata = %v, want exactly one value", values)
	}
	if values[0] != "Bearer s3cret-token" {
		t.Fatalf("authorization = %q, want %q", values[0], "Bearer s3cret-token")
	}
	if !strings.HasPrefix(values[0], "Bearer ") {
		t.Fatalf("authorization %q must use the Bearer scheme", values[0])
	}
}

// Every RPC on the connection carries it, not only the first: gRPC asks the
// credential per call, and the scheduler checks per call.
func TestSchedulerBearerTokenIsPresentedOnEveryCall(t *testing.T) {
	addr, seen := startRecordingScheduler(t)
	client := dialScheduler(t, addr, "again")

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})
		cancel()
		if err != nil {
			t.Fatalf("LookupNode %d: %v", i, err)
		}
		if got := receiveMetadata(t, seen).Get("authorization"); len(got) != 1 || got[0] != "Bearer again" {
			t.Fatalf("call %d authorization = %v, want [Bearer again]", i, got)
		}
	}
}

// The link is plaintext today. A credential that demanded transport security
// would make grpc refuse the dial, which reads as "the scheduler is down"
// rather than "the token needs TLS".
func TestSchedulerBearerTokenDoesNotDemandTransportSecurity(t *testing.T) {
	if schedulerBearerToken("x").RequireTransportSecurity() {
		t.Fatal("RequireTransportSecurity() = true; the gateway dials the scheduler insecurely and would be refused")
	}
}
