package scheduler

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The fleet shape is set from the command line rather than baked in, so one
// binary can sweep it and the in-memory and Redis stores are always measured
// on the same axis:
//
//	go test ./scheduler/internal -run XXX -bench 'Reconcile' -fleet.nodes=1000 -fleet.churn=0.02
//
// The defaults are the shape the H2 design named.
var (
	fleetNodes     = flag.Int("fleet.nodes", 10_000, "nodes in the synthetic fleet")
	fleetSandboxes = flag.Int("fleet.sandboxes", 500, "sandboxes on every node")
	fleetBindings  = flag.Int("fleet.bindings", 1_000_000, "bindings the store holds in the lookup benchmark")
	// Churn is what makes reconcile do anything beyond refreshing: every
	// heartbeat retires this fraction of the node's roster and admits as many
	// new sandboxes.
	fleetChurn = flag.Float64("fleet.churn", 0, "fraction of a node's roster replaced on every heartbeat")
	// Staleness is the D5 race made adjustable. A node collects its roster,
	// then the gateway places a sandbox on it, then the heartbeat arrives:
	// the sandbox is bound but unlisted. This is how long before the heartbeat
	// those bindings were recorded. Below the grace they must survive; at or
	// past it the roster is believed and they are lost.
	fleetStaleness = flag.Duration("fleet.staleness", 0, "how long before the heartbeat the arrivals its roster omits were recorded")
	fleetGrace     = flag.Duration("fleet.grace", defaultReconcileGracePeriod, "reconcile grace the binding store is built with")
)

const (
	benchClusterID = "cluster-sim"
	// benchBindingTTL keeps everything the benchmarks write alive for the whole
	// run. The reconcile benchmark backdates its seed bindings past the grace
	// so the departures churn drives are deleted rather than retained; at the
	// shipped 30s TTL those would lapse partway through a long -benchtime and
	// turn into "absent" outcomes that measure nothing.
	benchBindingTTL = time.Hour
	benchReportTTL  = time.Hour
	// benchCPUConfigJSON is a valid, empty Firecracker CPU config. Nodes send
	// one on every heartbeat, and a fleet where they all have arrived is the
	// steady state: the registry then holds a computed intersection and stops
	// walking every observed node on every heartbeat to see whether it can.
	benchCPUConfigJSON = `{"kvm_capabilities":[],"cpuid_modifiers":[],"msr_modifiers":[]}`
)

// benchBackend is one binding store under measurement plus the one operation
// the stores do not expose: writing a binding as if it had been recorded at
// some earlier moment. Reconcile decides by that stamp, so a benchmark of its
// paths has to place bindings on either side of the grace without waiting the
// grace out.
type benchBackend struct {
	name  string
	store BindingStore
	// backdate establishes every id on node with the given stamp.
	backdate func(tb testing.TB, ids []string, node Node, recordedAt time.Time)
}

type benchBackendFactory struct {
	name string
	// build skips the caller when the backend is unavailable, which is how
	// the Redis half behaves without a redis-server.
	build func(tb testing.TB, grace time.Duration) benchBackend
}

var benchBackendFactories = []benchBackendFactory{
	{name: "memory", build: memoryBenchBackend},
	{name: "redis", build: redisBenchBackend},
}

func memoryBenchBackend(tb testing.TB, grace time.Duration) benchBackend {
	tb.Helper()
	store := NewInMemoryBindingStoreWithGrace(benchBindingTTL, grace)
	return benchBackend{
		name:  "memory",
		store: store,
		backdate: func(tb testing.TB, ids []string, node Node, recordedAt time.Time) {
			for _, id := range ids {
				if err := store.Record(id, node, recordedAt); err != nil {
					tb.Fatalf("record %s: %v", id, err)
				}
			}
		},
	}
}

// redisBenchBackend needs a redis-server: REDIS_SERVER_BIN, or one on PATH.
// Without one every Redis benchmark skips, and says so; a benchmark is
// allowed to, where a Redis test is not.
func redisBenchBackend(tb testing.TB, grace time.Duration) benchBackend {
	tb.Helper()
	addr := startRedisServerForTest(tb)
	store, err := NewRedisBindingStore(addr, benchBindingTTL)
	if err != nil {
		tb.Fatalf("create redis binding store: %v", err)
	}
	store.reconcileGrace = grace
	tb.Cleanup(func() { _ = store.Close() })
	return benchBackend{
		name:  "redis",
		store: store,
		backdate: func(tb testing.TB, ids []string, node Node, recordedAt time.Time) {
			// The same record redisSetBindingScript writes, stamped here
			// instead of from redis TIME. Both clocks are this host's, which
			// is the one case comparing them is safe. The index entry is what
			// makes reconcile consider the binding at all.
			ctx := context.Background()
			pipe := store.client.Pipeline()
			nodeKey := store.nodeKey(node.ID)
			for _, id := range ids {
				value, err := json.Marshal(redisBindingRecord{Node: node, RecordedAtMs: recordedAt.UnixMilli()})
				if err != nil {
					tb.Fatalf("marshal binding %s: %v", id, err)
				}
				pipe.Set(ctx, store.bindingKey(id), value, benchBindingTTL)
				pipe.SAdd(ctx, nodeKey, id)
			}
			pipe.PExpire(ctx, nodeKey, store.nodeIndexTTL())
			if _, err := pipe.Exec(ctx); err != nil {
				tb.Fatalf("backdate %d bindings on %s: %v", len(ids), node.ID, err)
			}
		},
	}
}

// benchNodes builds a fleet of discoverable nodes and the incarnation each
// one reports.
func benchNodes(count int) ([]Node, []string) {
	nodes := make([]Node, 0, count)
	incarnations := make([]string, 0, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("node-%06d", i)
		nodes = append(nodes, Node{ID: id, Endpoint: "http://" + id + ":8080"})
		incarnations = append(incarnations, fmt.Sprintf("0199c000-0000-7000-8000-%012d", i))
	}
	return nodes, incarnations
}

// benchSandboxID is UUID-shaped so key sizes in Redis, and string sizes in
// memory, are what production pays.
func benchSandboxID(i int) string {
	return fmt.Sprintf("%08x-0000-7000-8000-%012x", i, i)
}

// Every proxied request resolves its sandbox through LookupNode, so this is
// the scheduler's hottest read, and the one that has to hold up as the store
// fills.
func BenchmarkLookupNode_1MBindings(b *testing.B) {
	for _, factory := range benchBackendFactories {
		b.Run("store="+factory.name, func(b *testing.B) {
			backend := factory.build(b, *fleetGrace)
			count := *fleetBindings
			// Enough nodes that no node index is a single million-member set.
			nodes, _ := benchNodes(max(1, count/500))
			service := NewService(zap.NewNop(), NewAtomicNodeRegistry(nodes, benchReportTTL),
				NewStrategy("round_robin"), backend.store, WithBindingTTL(benchBindingTTL))
			ids := seedLookupBindings(b, service, nodes, count)
			ctx := context.Background()

			// The store is seeded once, above, and the leaves only read: the
			// leaf bodies run again at every b.N step and must not re-seed.
			b.Run("serial", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					lookupBenchID(b, service, ctx, ids[i%len(ids)])
				}
			})
			b.Run("parallel", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					// Each worker walks its own stride so the workers do not
					// all hit the same key at the same moment.
					i := 0
					for pb.Next() {
						lookupBenchID(b, service, ctx, ids[i%len(ids)])
						i += 7919
					}
				})
			})
		})
	}
}

func lookupBenchID(tb testing.TB, service *Service, ctx context.Context, id string) {
	resp, err := service.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: id})
	if err != nil {
		tb.Fatalf("LookupNode %s: %v", id, err)
	}
	if resp.GetNode().GetNodeId() == "" {
		tb.Fatalf("LookupNode %s resolved to no node", id)
	}
}

// seedLookupBindings writes count bindings through RecordAssignments, the
// batched write the gateway uses, spread across every node.
func seedLookupBindings(tb testing.TB, service *Service, nodes []Node, count int) []string {
	tb.Helper()
	const batchSize = 1000
	ctx := context.Background()
	start := time.Now()
	ids := make([]string, count)
	for from := 0; from < count; from += batchSize {
		to := min(from+batchSize, count)
		req := &schedulerv1.RecordAssignmentsRequest{
			Assignments: make([]*schedulerv1.RecordAssignmentRequest, 0, to-from),
		}
		for i := from; i < to; i++ {
			ids[i] = benchSandboxID(i)
			req.Assignments = append(req.Assignments, &schedulerv1.RecordAssignmentRequest{
				SandboxId: ids[i],
				Node:      nodes[i%len(nodes)].ToProto(),
			})
		}
		resp, err := service.RecordAssignments(ctx, req)
		if err != nil {
			tb.Fatalf("RecordAssignments: %v", err)
		}
		for _, result := range resp.GetResults() {
			if result.GetError() != "" {
				tb.Fatalf("RecordAssignments %s: %s", result.GetSandboxId(), result.GetError())
			}
		}
	}
	tb.Logf("seeded %d bindings across %d nodes in %s", count, len(nodes), time.Since(start).Round(time.Millisecond))
	return ids
}

// nodeRoster is what one simulated node believes it holds, oldest first.
type nodeRoster struct {
	live []string
	// minted counts the arrivals so far, so their ids never collide with the
	// seed's or each other's.
	minted int
}

// turnOver retires the oldest churn sandboxes and admits as many new ones,
// returning the arrivals so the caller can decide whether the roster it sends
// has caught up with them yet.
func (r *nodeRoster) turnOver(nodeID string, churn int) []string {
	churn = min(churn, len(r.live))
	if churn == 0 {
		return nil
	}
	remaining := make([]string, 0, len(r.live))
	remaining = append(remaining, r.live[churn:]...)
	arrived := make([]string, 0, churn)
	for i := 0; i < churn; i++ {
		arrived = append(arrived, fmt.Sprintf("%s-new-%08d", nodeID, r.minted))
		r.minted++
	}
	r.live = append(remaining, arrived...)
	return arrived
}

// reconcileFleet is a synthetic fleet arranged for the heartbeat benchmark:
// every node is observed by the registry, and holds bindings in the store that
// were all established long enough ago to be past the reconcile grace.
type reconcileFleet struct {
	service      *Service
	backend      benchBackend
	nodes        []Node
	incarnations []string
	rosters      []nodeRoster
	stamps       []int64
	machine      *schedulerv1.MachineInfo
}

func newReconcileFleet(tb testing.TB, factory benchBackendFactory, nodeCount int, sandboxesPerNode int, grace time.Duration) *reconcileFleet {
	tb.Helper()
	backend := factory.build(tb, grace)
	nodes, incarnations := benchNodes(nodeCount)
	registry := NewAtomicNodeRegistry(nodes, benchReportTTL)
	fleet := &reconcileFleet{
		service: NewService(zap.NewNop(), registry, NewStrategy("round_robin"), backend.store,
			WithBindingTTL(benchBindingTTL), WithReportTTL(benchReportTTL)),
		backend:      backend,
		nodes:        nodes,
		incarnations: incarnations,
		rosters:      make([]nodeRoster, nodeCount),
		stamps:       make([]int64, nodeCount),
		machine:      &schedulerv1.MachineInfo{CpuArchitecture: "x86_64", CpuConfigJson: benchCPUConfigJSON},
	}

	start := time.Now()
	// Two passes, the first without a CPU config. The registry recomputes the
	// cluster's CPU intersection on every join that carries one, parsing every
	// config it holds, so a fleet joining with configs costs O(nodes^2) parses
	// — about 50M at 10k nodes. Joining first and reporting the config after
	// leaves one computation at the end, and the same steady state.
	now := time.Now()
	for pass := 0; pass < 2; pass++ {
		for i, node := range nodes {
			req := fleet.heartbeat(i, nil, false)
			if pass == 0 {
				req.MachineInfo = nil
			}
			if _, _, err := registry.Heartbeat(req, now); err != nil {
				tb.Fatalf("seed heartbeat for %s: %v", node.ID, err)
			}
		}
	}

	// Established past the grace, so a roster that omits one of these is
	// believed and the binding deleted: that is the reconcile path a departure
	// takes in a fleet that has been running longer than the grace.
	established := time.Now().Add(-(grace + time.Minute))
	for i, node := range nodes {
		ids := make([]string, 0, sandboxesPerNode)
		for j := 0; j < sandboxesPerNode; j++ {
			ids = append(ids, fmt.Sprintf("%s-sbx-%05d", node.ID, j))
		}
		backend.backdate(tb, ids, node, established)
		fleet.rosters[i] = nodeRoster{live: ids}
	}
	tb.Logf("seeded %d nodes x %d sandboxes on %s in %s", nodeCount, sandboxesPerNode, backend.name, time.Since(start).Round(time.Millisecond))
	return fleet
}

// heartbeat builds node i's report listing roster. A nil roster with elided
// set is a digest-only heartbeat; the digest is the caller's.
func (f *reconcileFleet) heartbeat(i int, roster []string, elided bool) *schedulerv1.HeartbeatRequest {
	f.stamps[i]++
	return &schedulerv1.HeartbeatRequest{
		NodeId:            f.nodes[i].ID,
		ClusterId:         benchClusterID,
		ServiceInstanceId: f.incarnations[i],
		MachineInfo:       f.machine,
		SandboxIds:        roster,
		RosterComplete:    !elided,
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:           schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount:     uint32(len(roster)),
			CpuCount:         64,
			AllocatedCpu:     uint32(len(roster)),
			MemoryTotalBytes: 1 << 40,
			MemoryUsedBytes:  uint64(len(roster)) << 30,
			ReportedAtUnixMs: f.stamps[i],
		},
	}
}

// nextHeartbeat advances node i by one heartbeat period: churn sandboxes
// depart and as many arrive. With staleness the arrivals were bound by the
// gateway that long ago and the roster has not caught up with them, which is
// the window the reconcile grace exists to cover.
func (f *reconcileFleet) nextHeartbeat(tb testing.TB, i int, churn int, staleness time.Duration, sample *arrivalSample) *schedulerv1.HeartbeatRequest {
	node := f.nodes[i]
	arrived := f.rosters[i].turnOver(node.ID, churn)
	listed := f.rosters[i].live
	if staleness > 0 && len(arrived) > 0 {
		f.backend.backdate(tb, arrived, node, time.Now().Add(-staleness))
		listed = listed[:len(listed)-len(arrived)]
		sample.add(node.ID, arrived)
	}
	return f.heartbeat(i, listed, false)
}

// reconcileOutcomes sums the per-node reconcile counters over the fleet, so a
// run can report what its heartbeats did to the bindings they omitted.
func (f *reconcileFleet) reconcileOutcomes(tb testing.TB) map[string]float64 {
	tb.Helper()
	totals := make(map[string]float64, 2)
	for _, outcome := range []string{reconcileOutcomeDeleted, reconcileOutcomeRetained} {
		for _, node := range f.nodes {
			counter, err := schedulerReconcileBindingsTotal.GetMetricWithLabelValues(node.ID, outcome)
			if err != nil {
				tb.Fatalf("reconcile counter %s/%s: %v", node.ID, outcome, err)
			}
			metric := &dto.Metric{}
			if err := counter.Write(metric); err != nil {
				tb.Fatalf("read reconcile counter %s/%s: %v", node.ID, outcome, err)
			}
			totals[outcome] += metric.GetCounter().GetValue()
		}
	}
	return totals
}

func (f *reconcileFleet) reportOutcomes(b *testing.B, before map[string]float64) {
	after := f.reconcileOutcomes(b)
	for _, outcome := range []string{reconcileOutcomeDeleted, reconcileOutcomeRetained} {
		b.ReportMetric((after[outcome]-before[outcome])/float64(b.N), outcome+"/op")
	}
}

// arrivalSample keeps the most recent arrivals a run recorded but omitted from
// their rosters, so the run can report how many of them the heartbeats lost.
// Every omitted arrival meets the same decision on the heartbeat that omits
// it, so a bounded sample is representative.
type arrivalSample struct {
	ids   []string
	nodes []string
	next  int
	seen  int
}

func newArrivalSample(capacity int) *arrivalSample {
	return &arrivalSample{ids: make([]string, capacity), nodes: make([]string, capacity)}
}

func (s *arrivalSample) add(nodeID string, ids []string) {
	for _, id := range ids {
		s.ids[s.next] = id
		s.nodes[s.next] = nodeID
		s.next = (s.next + 1) % len(s.ids)
		s.seen++
	}
}

// lostFraction is the share of sampled arrivals LookupNode can no longer
// resolve to the node the gateway bound them to.
func (s *arrivalSample) lostFraction(tb testing.TB, service *Service) float64 {
	tb.Helper()
	checked := min(s.seen, len(s.ids))
	if checked == 0 {
		return 0
	}
	ctx := context.Background()
	lost := 0
	for i := 0; i < checked; i++ {
		resp, err := service.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: s.ids[i]})
		switch {
		case status.Code(err) == codes.NotFound:
			lost++
		case err != nil:
			tb.Fatalf("LookupNode %s: %v", s.ids[i], err)
		case resp.GetNode().GetNodeId() != s.nodes[i]:
			lost++
		}
	}
	return float64(lost) / float64(checked)
}

func churnCount(sandboxesPerNode int, fraction float64) int {
	if fraction <= 0 {
		return 0
	}
	return int(math.Ceil(fraction * float64(sandboxesPerNode)))
}

// Every node heartbeats every few seconds, and every heartbeat reconciles the
// node's whole roster against the store, so this is the scheduler's steady
// write load. The two stores are driven identically so their costs compare;
// the fleet shape and the churn, staleness and grace knobs are flags above.
//
// Heartbeats rotate through the fleet rather than repeating one node, so the
// per-node state they touch is as cold as it is on a scheduler hearing from
// every node in turn. Full rosters are sent without a digest, which is what a
// node whose roster changed, or a scheduler that restarted, costs; the
// elided leaf measures the digest-only heartbeat a stable roster settles into.
func BenchmarkHeartbeatReconcile_10kNodes_500Sandboxes(b *testing.B) {
	for _, factory := range benchBackendFactories {
		b.Run("store="+factory.name, func(b *testing.B) {
			fleet := newReconcileFleet(b, factory, *fleetNodes, *fleetSandboxes, *fleetGrace)
			ctx := context.Background()
			churn := churnCount(*fleetSandboxes, *fleetChurn)

			b.Run("roster=full", func(b *testing.B) {
				before := fleet.reconcileOutcomes(b)
				sample := newArrivalSample(4096)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					req := fleet.nextHeartbeat(b, i%len(fleet.nodes), churn, *fleetStaleness, sample)
					if _, err := fleet.service.Heartbeat(ctx, req); err != nil {
						b.Fatalf("Heartbeat %s: %v", req.GetNodeId(), err)
					}
				}
				b.StopTimer()
				fleet.reportOutcomes(b, before)
				if *fleetStaleness > 0 {
					b.ReportMetric(sample.lostFraction(b, fleet.service), "lost_frac")
				}
			})

			// A roster that changes every heartbeat can never be elided, so
			// the digest-only path only exists at zero churn.
			if churn > 0 {
				return
			}
			b.Run("roster=elided", func(b *testing.B) {
				// Priming is a full send per node, so it is bounded and paid
				// before the timer rather than once per node in the fleet.
				primed := min(len(fleet.nodes), 256)
				digests := make([]string, primed)
				for i := 0; i < primed; i++ {
					roster := fleet.rosters[i].live
					digests[i] = RosterDigest(roster)
					req := fleet.heartbeat(i, roster, false)
					req.RosterDigest = digests[i]
					req.RosterFull = true
					resp, err := fleet.service.Heartbeat(ctx, req)
					if err != nil {
						b.Fatalf("priming Heartbeat %s: %v", req.GetNodeId(), err)
					}
					if !resp.GetRosterDigestAccepted() {
						b.Fatalf("scheduler refused to let %s elide its roster", req.GetNodeId())
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					n := i % primed
					req := fleet.heartbeat(n, nil, true)
					req.RosterDigest = digests[n]
					resp, err := fleet.service.Heartbeat(ctx, req)
					if err != nil {
						b.Fatalf("Heartbeat %s: %v", req.GetNodeId(), err)
					}
					if resp.GetRequestFullRoster() {
						b.Fatalf("scheduler lost %s's cached roster", req.GetNodeId())
					}
				}
			})
		})
	}
}
