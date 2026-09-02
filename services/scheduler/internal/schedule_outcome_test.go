package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// counterValue reads a counter, for asserting what one operation counted
// without a registry of its own.
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var metric dto.Metric
	if err := c.Write(&metric); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return metric.GetCounter().GetValue()
}

// histogramSum reads a histogram's running sum and count.
func histogramSum(t *testing.T, h prometheus.Histogram) (sum float64, count uint64) {
	t.Helper()
	var metric dto.Metric
	if err := h.Write(&metric); err != nil {
		t.Fatalf("read histogram: %v", err)
	}
	return metric.GetHistogram().GetSampleSum(), metric.GetHistogram().GetSampleCount()
}

// No fleet and no capacity are different answers. The first is a deployment
// with nothing behind it; the second is a moment's worth of full nodes, which
// a retry may find otherwise — so the gateway can attach a Retry-After to one
// and not the other.
func TestScheduleDistinguishesNoNodesFromNoEligibleNodes(t *testing.T) {
	ctx := context.Background()
	one := uint32(1)

	empty := NewService(nil, NewAtomicNodeRegistry(nil, time.Minute), NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute))
	if _, err := empty.Schedule(ctx, &schedulerv1.ScheduleRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("no discovered nodes: err = %v, want Unavailable", err)
	}

	full := NewService(nil,
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Minute),
		NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute),
		WithNodeResourceLimit(&config.NodeResourceLimit{MaxSandboxCount: &one}))
	beat := readyHeartbeat("node-a")
	beat.Snapshot.SandboxCount = 5
	if _, err := full.Heartbeat(ctx, beat); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	_, err := full.Schedule(ctx, &schedulerv1.ScheduleRequest{})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("every node over its limit: err = %v, want ResourceExhausted", err)
	}
	if msg := status.Convert(err).Message(); msg != "no node satisfies the configured resource limits" {
		t.Fatalf("message = %q, want it to name the resource limits", msg)
	}

	refused := NewService(nil,
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Minute),
		NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute))
	if _, err := refused.Heartbeat(ctx, readyHeartbeat("node-a")); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	_, err = refused.Schedule(ctx, &schedulerv1.ScheduleRequest{ExcludeNodeIds: []string{"node-a"}})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("every node already refused: err = %v, want ResourceExhausted", err)
	}
	if msg := status.Convert(err).Message(); msg != "every eligible node has already refused this sandbox" {
		t.Fatalf("message = %q, want it to name the exclusions", msg)
	}
}

func healthGateService(t *testing.T, ttl time.Duration, nodes ...string) (*Service, *AtomicNodeRegistry) {
	t.Helper()
	discovered := make([]Node, 0, len(nodes))
	for _, id := range nodes {
		discovered = append(discovered, Node{ID: id, Endpoint: "http://" + id})
	}
	registry := NewAtomicNodeRegistry(discovered, ttl)
	service := NewService(nil, registry, NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute),
		WithReportTTL(ttl))
	return service, registry
}

func placements(t *testing.T, service *Service, n int) map[string]int {
	t.Helper()
	got := make(map[string]int)
	for i := 0; i < n; i++ {
		resp, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
		if err != nil {
			t.Fatalf("placement %d: %v", i, err)
		}
		got[resp.GetNode().GetNodeId()]++
	}
	return got
}

// The SIGSTOP-a-node case: one node stops heartbeating, and every create lands
// on the one that did not, rather than 1/N of them going to a node that will
// never answer.
func TestScheduleSkipsNodesPastReportTTL(t *testing.T) {
	const ttl = time.Second
	service, registry := healthGateService(t, ttl, "node-a", "node-b")
	start := time.Now()

	for _, id := range []string{"node-a", "node-b"} {
		if _, _, err := registry.Heartbeat(readyHeartbeat(id), start); err != nil {
			t.Fatalf("heartbeat %s: %v", id, err)
		}
	}
	// node-a keeps reporting, well past the TTL that node-b's last report ages
	// through by the time Schedule runs.
	if _, _, err := registry.Heartbeat(readyHeartbeat("node-a"), start.Add(2*ttl)); err != nil {
		t.Fatalf("refresh node-a: %v", err)
	}
	registry.observed["node-b"] = observedNodeRecord{
		node:      registry.observed["node-b"].node,
		reportTTL: ttl,
	}
	registry.observed["node-b"].node.LastSeenUnixMs = start.Add(-2 * ttl).UnixMilli()

	got := placements(t, service, 10)
	if got["node-b"] != 0 || got["node-a"] != 10 {
		t.Fatalf("placements = %v, want all ten on the node that is still heartbeating", got)
	}
}

// Every node stale at once is a scheduler that cannot hear its fleet, not a
// fleet that died; refusing all placement would turn a blip into an outage.
// The fail-open is counted so it is visible when it happens.
func TestScheduleFailsOpenWhenEveryNodeIsStale(t *testing.T) {
	service, _ := healthGateService(t, time.Minute, "node-a", "node-b")
	before := counterValue(t, schedulerScheduleFailOpenTotal)

	// Nothing has heartbeated: the restart window, in which the registry is
	// empty and every node counts as unseen.
	got := placements(t, service, 4)
	if len(got) == 0 {
		t.Fatal("no placements succeeded")
	}
	if delta := counterValue(t, schedulerScheduleFailOpenTotal) - before; delta != 4 {
		t.Fatalf("fail-open counted %v times, want once per placement", delta)
	}
}

// Partial staleness fails closed, and is not counted as failing open.
func TestScheduleFailsClosedWhenSomeNodesAreFresh(t *testing.T) {
	service, registry := healthGateService(t, time.Minute, "node-a", "node-b")
	if _, _, err := registry.Heartbeat(readyHeartbeat("node-a"), time.Now()); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	before := counterValue(t, schedulerScheduleFailOpenTotal)

	got := placements(t, service, 6)
	if got["node-b"] != 0 {
		t.Fatalf("placements = %v, want none on the node that never heartbeated", got)
	}
	if delta := counterValue(t, schedulerScheduleFailOpenTotal) - before; delta != 0 {
		t.Fatalf("fail-open counted %v times with a fresh node present", delta)
	}
}

// The node's own word about its state reaches placement. A node that reports
// itself draining, or unhealthy, takes no new sandboxes from the next
// placement on, and takes them again the moment it reports ready. No
// discovery change is involved: this is the heartbeat status alone.
func TestSelfReportedDrainingAndUnhealthyNodesAreExcludedFromPlacement(t *testing.T) {
	for _, reported := range []schedulerv1.NodeStatus{
		schedulerv1.NodeStatus_NODE_STATUS_LINGERING,
		schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY,
	} {
		t.Run(reported.String(), func(t *testing.T) {
			service, _ := healthGateService(t, time.Minute, "node-a", "node-b")
			ctx := context.Background()
			if _, err := service.Heartbeat(ctx, readyHeartbeat("node-a")); err != nil {
				t.Fatalf("heartbeat a: %v", err)
			}
			draining := readyHeartbeat("node-b")
			draining.Snapshot.Status = reported
			if _, err := service.Heartbeat(ctx, draining); err != nil {
				t.Fatalf("heartbeat b: %v", err)
			}

			if got := placements(t, service, 6); got["node-b"] != 0 {
				t.Fatalf("placements = %v, want none on the node that reported %s", got, reported)
			}

			// GET /nodes agrees: the reported status is preserved for a node
			// discovery still lists.
			view, ok := service.nodes.GetObserved("node-b", "", time.Now())
			if !ok || view.GetSnapshot().GetStatus() != reported {
				t.Fatalf("node view status = %v, want the reported %s", view.GetSnapshot().GetStatus(), reported)
			}

			// Back to ready: schedulable again on the next placement.
			if _, err := service.Heartbeat(ctx, readyHeartbeat("node-b")); err != nil {
				t.Fatalf("heartbeat b ready: %v", err)
			}
			if got := placements(t, service, 6); got["node-b"] == 0 {
				t.Fatalf("placements = %v, want the recovered node back in rotation", got)
			}
		})
	}
}

// Round-robin's cycle is only a cycle over a list that stays put. The sample
// comes back unordered, and the strategy says whether that matters; when it
// does, the order is restored before selection.
func TestRoundRobinPlacementCyclesInStableOrder(t *testing.T) {
	service, _ := healthGateService(t, time.Minute, "node-c", "node-a", "node-b")
	ctx := context.Background()
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		if _, err := service.Heartbeat(ctx, readyHeartbeat(id)); err != nil {
			t.Fatalf("heartbeat %s: %v", id, err)
		}
	}

	want := []string{"node-a", "node-b", "node-c", "node-a", "node-b", "node-c"}
	for i, expected := range want {
		resp, err := service.Schedule(ctx, &schedulerv1.ScheduleRequest{})
		if err != nil {
			t.Fatalf("placement %d: %v", i, err)
		}
		if got := resp.GetNode().GetNodeId(); got != expected {
			t.Fatalf("placement %d went to %s, want %s: round-robin must cycle a stable order", i, got, expected)
		}
	}
}

// A strategy that draws or scores gets the candidates as the registry holds
// them: the sort is skipped rather than paid for and ignored.
func TestSampleNodesIsUnorderedWhenNoStrategyNeedsOrder(t *testing.T) {
	nodes := make([]Node, 0, 64)
	for i := 0; i < 64; i++ {
		id := nodeIDForIndex(i)
		nodes = append(nodes, Node{ID: id, Endpoint: "http://" + id})
	}
	registry := NewAtomicNodeRegistry(nodes, time.Minute)

	sortedEveryTime := true
	for attempt := 0; attempt < 8 && sortedEveryTime; attempt++ {
		got := registry.SampleNodes(0, false)
		if len(got) != 64 {
			t.Fatalf("whole fleet requested, got %d", len(got))
		}
		for i := 1; i < len(got); i++ {
			if got[i-1].ID > got[i].ID {
				sortedEveryTime = false
				break
			}
		}
	}
	if sortedEveryTime {
		t.Fatal("SampleNodes returned a sorted fleet on every call; the sort is meant to be the strategy's to ask for")
	}
	if sorted := registry.Snapshot(false); sorted[0].ID != nodeIDForIndex(0) || sorted[63].ID != nodeIDForIndex(63) {
		t.Fatal("Snapshot must stay sorted for listings")
	}
}

// Roster elision is withheld when the report TTL cannot cover the node's
// interval, as it already is for the binding TTL: a node the scheduler cannot
// hear three times per TTL gets no shortcuts.
func TestElisionIsWithheldWhenTheReportTTLIsTooShortForTheInterval(t *testing.T) {
	for _, tc := range []struct {
		name      string
		reportTTL time.Duration
		want      bool
	}{
		{name: "report ttl under three intervals withholds", reportTTL: 30 * time.Second, want: false},
		{name: "report ttl at three intervals permits", reportTTL: 45 * time.Second, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "10.0.0.1:8000"}}, tc.reportTTL)
			service := NewService(nil, registry, NewStrategy("round_robin"),
				NewInMemoryBindingStoreWithGrace(time.Minute, 0),
				WithBindingTTL(time.Minute), WithReportTTL(tc.reportTTL))
			if got := elisionPermitted(t, service, 15_000); got != tc.want {
				t.Fatalf("elision permitted = %v, want %v with report_ttl %s and a 15s interval", got, tc.want, tc.reportTTL)
			}
		})
	}
}
