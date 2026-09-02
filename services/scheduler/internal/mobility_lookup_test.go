package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mobilityLookupService(t *testing.T, nodes ...Node) *Service {
	t.Helper()
	return NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry(nodes, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)
}

func seedMobilityRecord(t *testing.T, service *Service, record MobilityRecord) {
	t.Helper()
	if _, err := service.mobility.Upsert(context.Background(), record); err != nil {
		t.Fatalf("seed mobility record: %v", err)
	}
}

// A paused sandbox with no binding is not a missing sandbox.
//
// Its binding lapses on the TTL like any other, and until now that turned a
// perfectly resumable sandbox into a 404 for a sandbox the client can still
// list. Driven through LookupNode, not the helper it calls, because the value
// of the fallback is entirely in it being reached from the RPC.
func TestLookupNodePlacesACommittedPausedSandbox(t *testing.T) {
	service := mobilityLookupService(t, Node{ID: "node-a", Endpoint: "http://node-a"})
	seedMobilityRecord(t, service, MobilityRecord{
		SandboxID:    "sbx-committed",
		OriginNodeID: "node-gone",
		Generation:   "0193-a",
		SnapshotID:   "snap-1",
		State:        MobilityParked,
		PausedAtMs:   time.Now().UnixMilli(),
	})

	resp, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{
		SandboxId: "sbx-committed",
	})
	if err != nil {
		t.Fatalf("lookup of a committed paused sandbox failed: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-a" {
		t.Fatalf("expected placement onto node-a, got %q", got)
	}

	// The placement must be written, or two concurrent lookups place one
	// sandbox on two nodes and the next lookup repeats the work.
	node, ok, err := service.store.Get("sbx-committed", time.Now())
	if err != nil || !ok {
		t.Fatalf("placement was not recorded as a binding (ok=%v err=%v)", ok, err)
	}
	if node.ID != "node-a" {
		t.Fatalf("recorded binding names %q, not the placed node", node.ID)
	}
}

// An uncommitted paused sandbox can only be served where it was written.
//
// Its state never reached the repository, so placing it on any node that
// happened to be free would send the caller to a node that cannot open it.
func TestLookupNodeSendsAnUncommittedPausedSandboxToItsOrigin(t *testing.T) {
	service := mobilityLookupService(t,
		Node{ID: "node-a", Endpoint: "http://node-a"},
		Node{ID: "node-origin", Endpoint: "http://node-origin"},
	)
	seedMobilityRecord(t, service, MobilityRecord{
		SandboxID:    "sbx-local",
		OriginNodeID: "node-origin",
		Generation:   "0193-b",
		State:        MobilityParked,
		PausedAtMs:   time.Now().UnixMilli(),
	})

	resp, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{
		SandboxId: "sbx-local",
	})
	if err != nil {
		t.Fatalf("lookup of an uncommitted paused sandbox failed: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-origin" {
		t.Fatalf("an uncommitted sandbox must resolve to its origin, got %q", got)
	}
}

// And when the origin is gone, so is it.
//
// The alternative is placing it on a node that will fail to open a snapshot
// that does not exist, which turns a clear 404 into a confusing resume error.
func TestLookupNodeStill404sAnUncommittedSandboxWhoseOriginIsGone(t *testing.T) {
	service := mobilityLookupService(t, Node{ID: "node-a", Endpoint: "http://node-a"})
	seedMobilityRecord(t, service, MobilityRecord{
		SandboxID:    "sbx-orphan",
		OriginNodeID: "node-departed",
		Generation:   "0193-c",
		State:        MobilityParked,
		PausedAtMs:   time.Now().UnixMilli(),
	})

	_, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{
		SandboxId: "sbx-orphan",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound for an unreachable sandbox, got %v", err)
	}
}

// A sandbox mid-handover resolves to whoever holds it, not to a new placement.
func TestLookupNodeSendsAHeldSandboxToItsHolder(t *testing.T) {
	service := mobilityLookupService(t,
		Node{ID: "node-a", Endpoint: "http://node-a"},
		Node{ID: "node-holder", Endpoint: "http://node-holder"},
	)
	seedMobilityRecord(t, service, MobilityRecord{
		SandboxID:    "sbx-held",
		OriginNodeID: "node-a",
		HolderNodeID: "node-holder",
		Generation:   "0193-d",
		SnapshotID:   "snap-2",
		State:        MobilityParked,
		PausedAtMs:   time.Now().UnixMilli(),
	})

	resp, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{
		SandboxId: "sbx-held",
	})
	if err != nil {
		t.Fatalf("lookup of a held sandbox failed: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-holder" {
		t.Fatalf("a held sandbox must resolve to its holder, got %q", got)
	}
}

// A sandbox with no record at all is still a 404.
func TestLookupNodeStill404sAnUnknownSandbox(t *testing.T) {
	service := mobilityLookupService(t, Node{ID: "node-a", Endpoint: "http://node-a"})
	_, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{
		SandboxId: "sbx-never-existed",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound for an unknown sandbox, got %v", err)
	}
}

// A read replica must stay a pure read.
//
// It has neither a node registry nor a strategy, so it cannot place anything;
// the point of the gateway's fallback is that the primary does this instead.
func TestQueryOnlyLookupNodeDoesNotPlace(t *testing.T) {
	store := NewInMemoryBindingStore(defaultObservedReportTTL)
	service := NewQueryOnlyService(zap.NewNop(), store)
	_, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{
		SandboxId: "sbx-anything",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("a query-only replica must answer NotFound, got %v", err)
	}
}
