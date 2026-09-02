package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/protobuf/proto"
)

// fakeNodeStream is the bus without Redis: every subscriber sees every event,
// in publish order. Events go through the wire encoding, so a test can assert
// on what was actually sent rather than on the struct that was handed over.
type fakeNodeStream struct {
	mu        sync.Mutex
	subs      []func(*schedulerv1.NodeStateEvent)
	published [][]byte
}

func (b *fakeNodeStream) Publish(_ context.Context, _ string, ev *schedulerv1.NodeStateEvent) error {
	payload, err := proto.Marshal(ev)
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.published = append(b.published, payload)
	subs := append([]func(*schedulerv1.NodeStateEvent){}, b.subs...)
	b.mu.Unlock()

	for _, sub := range subs {
		decoded := &schedulerv1.NodeStateEvent{}
		if err := proto.Unmarshal(payload, decoded); err != nil {
			return err
		}
		sub(decoded)
	}
	return nil
}

func (b *fakeNodeStream) Subscribe(_ context.Context, fn func(*schedulerv1.NodeStateEvent)) (<-chan struct{}, error) {
	b.mu.Lock()
	b.subs = append(b.subs, fn)
	b.mu.Unlock()
	ready := make(chan struct{})
	close(ready)
	return ready, nil
}

func (b *fakeNodeStream) Close() error { return nil }

func (b *fakeNodeStream) events(t *testing.T) []*schedulerv1.NodeStateEvent {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()

	decoded := make([]*schedulerv1.NodeStateEvent, 0, len(b.published))
	for _, payload := range b.published {
		event := &schedulerv1.NodeStateEvent{}
		if err := proto.Unmarshal(payload, event); err != nil {
			t.Fatalf("unmarshal published event: %v", err)
		}
		decoded = append(decoded, event)
	}
	return decoded
}

func newStreamReplicas(t *testing.T, nodes []Node, ttl time.Duration, ids ...string) (*fakeNodeStream, []*StreamFedNodeRegistry) {
	t.Helper()

	bus := &fakeNodeStream{}
	replicas := make([]*StreamFedNodeRegistry, 0, len(ids))
	for _, id := range ids {
		replicas = append(replicas, joinStreamReplica(t, bus, nodes, ttl, id))
	}
	return bus, replicas
}

// joinStreamReplica starts one replica against a bus that may already be
// carrying traffic. The fake bus replays nothing, which is what a real replica
// sees once the events it missed have aged out of the stream's retention.
func joinStreamReplica(t *testing.T, bus NodeStreamBus, nodes []Node, ttl time.Duration, id string) *StreamFedNodeRegistry {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	registry := NewStreamFedNodeRegistry(NewAtomicNodeRegistry(nodes, ttl), bus, id, nil)
	if _, err := registry.Run(ctx); err != nil {
		t.Fatalf("subscribe %s: %v", id, err)
	}
	return registry
}

func streamNodes() []Node {
	return []Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}
}

// The reason the stream exists: a node's connection is sticky, so each replica
// takes the heartbeats of its own share of the fleet and has to be told the
// rest.
func TestStreamFedRegistryConverges(t *testing.T) {
	_, replicas := newStreamReplicas(t, streamNodes(), 30*time.Second, "replica-1", "replica-2")
	// The subscriber applies against the wall clock, because that is what a
	// running replica compares an event's stamp to, so the heartbeats are
	// stamped at a real time inside the window rather than at an epoch.
	now := time.Now().Add(-time.Second).Truncate(time.Millisecond)

	if _, _, err := replicas[0].Heartbeat(readyHeartbeat("node-a"), now); err != nil {
		t.Fatalf("heartbeat node-a: %v", err)
	}
	if _, _, err := replicas[1].Heartbeat(readyHeartbeat("node-b"), now); err != nil {
		t.Fatalf("heartbeat node-b: %v", err)
	}

	for i, replica := range replicas {
		for _, nodeID := range []string{"node-a", "node-b"} {
			snapshot, health := replica.PeekObservedHealth(nodeID)
			if !health.Seen {
				t.Fatalf("replica %d never saw %s", i, nodeID)
			}
			if health.LastSeenUnixMs != now.UTC().UnixMilli() {
				t.Fatalf("replica %d saw %s at %d, want %d", i, nodeID, health.LastSeenUnixMs, now.UTC().UnixMilli())
			}
			if snapshot.GetStatus() != schedulerv1.NodeStatus_NODE_STATUS_READY {
				t.Fatalf("replica %d has %s in status %v, want READY", i, nodeID, snapshot.GetStatus())
			}
		}
	}
}

// Delivery is at-least-once and unordered, so applying has to be idempotent and
// has to refuse to run backwards. Otherwise a redelivered event would move a
// node's freshness back and the health gate would drop a live node.
func TestStreamAppliesAreIdempotentAndUnordered(t *testing.T) {
	_, replicas := newStreamReplicas(t, streamNodes(), 30*time.Second, "replica-1")
	follower := replicas[0]
	now := time.Unix(1_000, 0)

	newer := &schedulerv1.NodeStateEvent{
		OriginReplicaId: "replica-2",
		LastSeenUnixMs:  now.UTC().UnixMilli(),
		Heartbeat:       readyHeartbeat("node-a"),
	}
	older := &schedulerv1.NodeStateEvent{
		OriginReplicaId: "replica-2",
		LastSeenUnixMs:  now.Add(-5 * time.Second).UTC().UnixMilli(),
		Heartbeat:       readyHeartbeat("node-a"),
	}

	follower.apply(newer, now)
	follower.apply(newer, now)
	follower.apply(older, now)

	_, health := follower.PeekObservedHealth("node-a")
	if health.LastSeenUnixMs != newer.GetLastSeenUnixMs() {
		t.Fatalf("last seen = %d, want the newest event's stamp %d", health.LastSeenUnixMs, newer.GetLastSeenUnixMs())
	}
}

// The stamp travels with the event and is applied as sent. Restamping on
// arrival would make a stream that had backed up look like a fleet that had
// just been heard from, which switches the health gate off for the whole fleet
// exactly when it is needed.
func TestStreamDoesNotRestampFreshness(t *testing.T) {
	ttl := 30 * time.Second
	_, replicas := newStreamReplicas(t, streamNodes(), ttl, "replica-1")
	follower := replicas[0]
	now := time.Unix(1_000, 0)

	follower.apply(&schedulerv1.NodeStateEvent{
		OriginReplicaId: "replica-2",
		LastSeenUnixMs:  now.Add(-40 * time.Second).UTC().UnixMilli(),
		Heartbeat:       readyHeartbeat("node-a"),
	}, now)

	snapshot, health := follower.PeekObservedHealth("node-a")
	healthy, dropped, failedOpen := FilterByHealth([]RichNode{{
		Node:     Node{ID: "node-a", Endpoint: "http://node-a"},
		Snapshot: snapshot,
		Health:   health,
	}}, ttl, now)
	if !failedOpen {
		t.Fatalf("health gate kept %d candidates, want the stale replicated node dropped", len(healthy))
	}
	if dropped[HealthFilterReasonStale] != 1 {
		t.Fatalf("dropped = %v, want one node dropped as stale", dropped)
	}
}

// Replica clocks are compared against each other, and a stamp from the future
// is a broken clock rather than an observation. Clamping keeps one such replica
// from making the whole fleet look permanently fresh.
func TestStreamClampsSkewedStamps(t *testing.T) {
	_, replicas := newStreamReplicas(t, streamNodes(), 30*time.Second, "replica-1")
	follower := replicas[0]
	now := time.Unix(1_000, 0)

	outcome, clamped := follower.applyRemote(&schedulerv1.NodeStateEvent{
		OriginReplicaId: "replica-2",
		LastSeenUnixMs:  now.Add(10 * time.Minute).UTC().UnixMilli(),
		Heartbeat:       readyHeartbeat("node-a"),
	}, now)
	if outcome != nodeStreamApplied || !clamped {
		t.Fatalf("applyRemote() = (%q, %v), want an applied and clamped event", outcome, clamped)
	}
	if _, health := follower.PeekObservedHealth("node-a"); health.LastSeenUnixMs != now.UTC().UnixMilli() {
		t.Fatalf("last seen = %d, want it clamped to now (%d)", health.LastSeenUnixMs, now.UTC().UnixMilli())
	}
}

// A spec lock. The roster is reconciled by the one replica that took the RPC,
// and putting it on the bus would make a stream that carries O(nodes) carry
// O(sandboxes) instead — the cost the roster digest exists to avoid. The
// emitted event count belongs to the replica that received those events.
func TestStreamNeverCarriesRosterOrEventCounts(t *testing.T) {
	bus, replicas := newStreamReplicas(t, streamNodes(), 30*time.Second, "replica-1")

	beat := readyHeartbeat("node-a")
	beat.SandboxIds = make([]string, 0, 500)
	for i := 0; i < 500; i++ {
		beat.SandboxIds = append(beat.SandboxIds, "sbx-"+string(rune('a'+i%26)))
	}
	beat.RosterDigest = "digest"
	beat.RosterFull = true
	beat.EmittedEventCount = 4242

	if _, _, err := replicas[0].Heartbeat(beat, time.Now()); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	events := bus.events(t)
	if len(events) != 1 {
		t.Fatalf("published %d events, want 1", len(events))
	}
	published := events[0].GetHeartbeat()
	if len(published.GetSandboxIds()) != 0 {
		t.Fatalf("published %d sandbox ids, want the roster stripped", len(published.GetSandboxIds()))
	}
	if published.GetRosterDigest() != "" || published.GetRosterFull() || published.GetRosterComplete() {
		t.Fatalf("published roster state %q/%v/%v, want it stripped",
			published.GetRosterDigest(), published.GetRosterFull(), published.GetRosterComplete())
	}
	if published.GetEmittedEventCount() != 0 {
		t.Fatalf("published emitted_event_count = %d, want it stripped", published.GetEmittedEventCount())
	}
	if published.GetNodeId() != "node-a" || published.GetServiceInstanceId() == "" {
		t.Fatal("published event lost the identity the receiving replica fences on")
	}
}

// The CPU configuration inside machine info is tens of kilobytes, so it rides
// the stream only when it changed. What the follower holds must not change with
// it: the carry-forward is what keeps the node's configuration on the record
// every heartbeat after the one that carried it.
func TestStreamPublishesMachineInfoOnlyWhenItChanges(t *testing.T) {
	bus, replicas := newStreamReplicas(t, streamNodes(), 30*time.Second, "replica-1", "replica-2")
	config := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFF)}},
	}}, nil)

	now := time.Now().Add(-5 * time.Second).Truncate(time.Millisecond)
	for i := 0; i < 2; i++ {
		beat := readyHeartbeat("node-a")
		beat.MachineInfo = &schedulerv1.MachineInfo{CpuArchitecture: "x86_64", CpuConfigJson: config}
		if _, _, err := replicas[0].Heartbeat(beat, now.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("heartbeat %d: %v", i, err)
		}
	}

	events := bus.events(t)
	if len(events) != 2 {
		t.Fatalf("published %d events, want 2", len(events))
	}
	if events[0].GetHeartbeat().GetMachineInfo().GetCpuConfigJson() != config {
		t.Fatal("the first event did not carry the machine info the follower has no other way to learn")
	}
	if events[1].GetHeartbeat().GetMachineInfo() != nil {
		t.Fatal("an unchanged machine info was republished")
	}

	observed, ok := replicas[1].GetObserved("node-a", "cluster", now.Add(time.Second))
	if !ok {
		t.Fatal("the follower has no record for node-a")
	}
	if observed.GetMachineInfo().GetCpuConfigJson() != config {
		t.Fatal("the follower dropped the CPU config when the second event elided it")
	}
}

// The intersection is computed per replica over the nodes that replica has
// heard from. Under sticky connections and no stream, each replica computes it
// over its own third of the fleet and hands nodes a CPU template more permissive
// than the fleet supports — which is the failure the intersection exists to
// prevent, and it is silent until a resumed sandbox executes an instruction its
// new host does not have.
func TestCPUIntersectionConvergesOnEveryReplica(t *testing.T) {
	nodes := []Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}
	_, replicas := newStreamReplicas(t, nodes, 30*time.Second, "replica-1", "replica-2")

	configs := map[string]string{
		"node-a": buildConfig(nil, []cpuidModifier{{
			Leaf: "0x1", Subleaf: "0x0", Flags: 0,
			Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xF0)}},
		}}, nil),
		"node-b": buildConfig(nil, []cpuidModifier{{
			Leaf: "0x1", Subleaf: "0x0", Flags: 0,
			Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0x3C)}},
		}}, nil),
	}

	now := time.Now().Add(-5 * time.Second).Truncate(time.Millisecond)
	// Each node heartbeats to the replica its connection landed on.
	for i, nodeID := range []string{"node-a", "node-b"} {
		beat := readyHeartbeat(nodeID)
		beat.MachineInfo = &schedulerv1.MachineInfo{CpuArchitecture: "x86_64", CpuConfigJson: configs[nodeID]}
		if _, _, err := replicas[i].Heartbeat(beat, now); err != nil {
			t.Fatalf("heartbeat %s: %v", nodeID, err)
		}
	}

	// The next heartbeat on each replica is what carries the intersection back
	// to the node, so that is where the two are compared.
	acks := make([]string, len(replicas))
	for i, nodeID := range []string{"node-a", "node-b"} {
		beat := readyHeartbeat(nodeID)
		beat.MachineInfo = &schedulerv1.MachineInfo{CpuArchitecture: "x86_64", CpuConfigJson: configs[nodeID]}
		_, ack, err := replicas[i].Heartbeat(beat, now.Add(time.Second))
		if err != nil {
			t.Fatalf("second heartbeat %s: %v", nodeID, err)
		}
		acks[i] = ack.CPUConfigJSON
	}

	if acks[0] == "" || acks[1] == "" {
		t.Fatalf("intersection = %q / %q, want both replicas to have one", acks[0], acks[1])
	}
	if acks[0] != acks[1] {
		t.Fatalf("replicas disagree on the CPU intersection:\n%s\n%s", acks[0], acks[1])
	}

	want, err := IntersectCpuConfigs([]string{configs["node-a"], configs["node-b"]})
	if err != nil {
		t.Fatalf("intersect: %v", err)
	}
	if acks[0] != want {
		t.Fatalf("intersection = %s, want the whole fleet's %s", acks[0], want)
	}
}

// Every other replication test subscribes its replicas before the first
// heartbeat, so machine info rides the very first event and the refresh never
// has to work. A replica that joins a tier already running — a scale-up, an HPA
// event, one restarted pod — is the shape that needs it: the events carrying
// machine info are older than the stream's retention, and every event after them
// elides the CPU configuration because it is tens of kilobytes. Without a
// refresh that fires at the shipped heartbeat interval, the joiner holds no
// configuration for any node pinned to its peers, and since the intersection is
// computed only once every observed node has one, a single such node suppresses
// it for the whole fleet: the joiner hands every node it serves no CPU template
// at all, and no metric says so.
func TestLateJoiningReplicaLearnsMachineInfoFromTheRefresh(t *testing.T) {
	// The interval nodes ship with: config/default.toml
	// [observability.scheduler_report] interval_secs = 5. Measuring the refresh
	// between two consecutive heartbeats never reaches the window.
	const heartbeatInterval = 5 * time.Second
	span := 2 * nodeStreamMachineInfoRefresh

	configs := map[string]string{
		"node-a": buildConfig(nil, []cpuidModifier{{
			Leaf: "0x1", Subleaf: "0x0", Flags: 0,
			Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xF0)}},
		}}, nil),
		"node-b": buildConfig(nil, []cpuidModifier{{
			Leaf: "0x1", Subleaf: "0x0", Flags: 0,
			Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0x3C)}},
		}}, nil),
	}
	beat := func(nodeID string) *schedulerv1.HeartbeatRequest {
		req := readyHeartbeat(nodeID)
		req.MachineInfo = &schedulerv1.MachineInfo{CpuArchitecture: "x86_64", CpuConfigJson: configs[nodeID]}
		return req
	}

	bus := &fakeNodeStream{}
	// A TTL wider than the whole simulated run, so no stamp is clamped: this
	// test is about what the events carry, not about freshness.
	ttl := 2 * span
	established := joinStreamReplica(t, bus, streamNodes(), ttl, "replica-1")

	// Stamps have to sit near the wall clock, because a subscriber applies an
	// event against its own clock.
	start := time.Now().Add(-span).Truncate(time.Millisecond)
	if _, _, err := established.Heartbeat(beat("node-a"), start); err != nil {
		t.Fatalf("heartbeat node-a: %v", err)
	}

	// replica-2 joins one heartbeat too late for the event that carried node-a's
	// machine info, and node-b's sticky connection lands on it.
	joiner := joinStreamReplica(t, bus, streamNodes(), ttl, "replica-2")

	var establishedAck, joinerAck string
	for at := start.Add(heartbeatInterval); !at.After(start.Add(span)); at = at.Add(heartbeatInterval) {
		_, ack, err := established.Heartbeat(beat("node-a"), at)
		if err != nil {
			t.Fatalf("heartbeat node-a at %s: %v", at.Sub(start), err)
		}
		establishedAck = ack.CPUConfigJSON

		_, ack, err = joiner.Heartbeat(beat("node-b"), at)
		if err != nil {
			t.Fatalf("heartbeat node-b at %s: %v", at.Sub(start), err)
		}
		joinerAck = ack.CPUConfigJSON
	}

	observed, ok := joiner.GetObserved("node-a", "cluster", time.Now())
	if !ok {
		t.Fatal("the late joiner never saw node-a at all")
	}
	if observed.GetMachineInfo().GetCpuConfigJson() != configs["node-a"] {
		t.Fatalf("the late joiner holds cpu_config_json=%q for node-a after %s of heartbeats every %s, want the configuration the refresh carries",
			observed.GetMachineInfo().GetCpuConfigJson(), span, heartbeatInterval)
	}

	want, err := IntersectCpuConfigs([]string{configs["node-a"], configs["node-b"]})
	if err != nil {
		t.Fatalf("intersect: %v", err)
	}
	if joinerAck != want {
		t.Fatalf("the late joiner handed node-b an intersection of %d bytes, want the whole fleet's %d",
			len(joinerAck), len(want))
	}
	if establishedAck != joinerAck {
		t.Fatalf("replicas disagree on the CPU intersection:\n%s\n%s", establishedAck, joinerAck)
	}
}

// The incarnation fence has to run on the replicated path too, or a report from
// a node process that has been replaced comes back in through the stream after
// being refused at the RPC.
func TestStreamRejectsAStaleIncarnation(t *testing.T) {
	_, replicas := newStreamReplicas(t, streamNodes(), 30*time.Second, "replica-1")
	follower := replicas[0]
	now := time.Unix(1_000, 0)

	live := readyHeartbeat("node-a")
	live.ServiceInstanceId = "01920000-0000-7000-8000-000000000002"
	follower.apply(&schedulerv1.NodeStateEvent{
		OriginReplicaId: "replica-2",
		LastSeenUnixMs:  now.UTC().UnixMilli(),
		Heartbeat:       live,
	}, now)

	superseded := readyHeartbeat("node-a")
	superseded.ServiceInstanceId = "01920000-0000-7000-8000-000000000001"
	superseded.Snapshot = &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY}
	outcome, _ := follower.applyRemote(&schedulerv1.NodeStateEvent{
		OriginReplicaId: "replica-2",
		LastSeenUnixMs:  now.Add(time.Second).UTC().UnixMilli(),
		Heartbeat:       superseded,
	}, now.Add(time.Second))

	if outcome != nodeStreamStale {
		t.Fatalf("applyRemote() = %q, want the superseded process refused", outcome)
	}
	if snapshot, _ := follower.PeekObservedHealth("node-a"); snapshot.GetStatus() != schedulerv1.NodeStatus_NODE_STATUS_READY {
		t.Fatalf("status = %v, want the live process's READY preserved", snapshot.GetStatus())
	}
}

// A replica whose discovery has not yet listed a node is not in an error state:
// it is starting. The event is counted and dropped, and the next one after the
// informer syncs lands.
func TestStreamDropsEventsForNodesDiscoveryHasNotNamed(t *testing.T) {
	_, replicas := newStreamReplicas(t, []Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second, "replica-1")
	follower := replicas[0]
	now := time.Unix(1_000, 0)

	outcome, _ := follower.applyRemote(&schedulerv1.NodeStateEvent{
		OriginReplicaId: "replica-2",
		LastSeenUnixMs:  now.UTC().UnixMilli(),
		Heartbeat:       readyHeartbeat("node-z"),
	}, now)
	if outcome != nodeStreamUnknownNode {
		t.Fatalf("applyRemote() = %q, want an undiscovered node dropped", outcome)
	}
	if _, health := follower.PeekObservedHealth("node-z"); health.Seen {
		t.Fatal("an undiscovered node was recorded as observed")
	}
}

// A replica must not apply what it published: the record is already there, and
// re-applying it would be one more copy of the same state to keep consistent.
func TestStreamSkipsItsOwnEcho(t *testing.T) {
	_, replicas := newStreamReplicas(t, streamNodes(), 30*time.Second, "replica-1")
	registry := replicas[0]
	now := time.Now()

	if _, _, err := registry.Heartbeat(readyHeartbeat("node-a"), now); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	rpc, stream := registry.ObservedSourceCounts()
	if rpc != 1 || stream != 0 {
		t.Fatalf("observed sources = (rpc %d, stream %d), want the node counted once, from the RPC", rpc, stream)
	}
}
