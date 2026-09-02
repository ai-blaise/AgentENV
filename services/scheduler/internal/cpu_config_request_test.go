package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

// cpuConfigHeartbeat is shaped like what the node sends: machine info on every
// heartbeat, carrying the CPU configuration only on the ones that report it.
func cpuConfigHeartbeat(nodeID string, cpuJSON string) *schedulerv1.HeartbeatRequest {
	beat := readyHeartbeat(nodeID)
	beat.MachineInfo = &schedulerv1.MachineInfo{CpuArchitecture: "x86_64", CpuConfigJson: cpuJSON}
	return beat
}

// The node sends its CPU configuration once per process. Until the scheduler
// holds one it must keep asking, and once it holds one it must stop: the
// payload is tens of kilobytes and the ask rides every heartbeat.
func TestHeartbeatAsksForCPUConfigUntilTheNodeSendsOne(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)

	_, ack, err := registry.Heartbeat(cpuConfigHeartbeat("node-a", ""), now)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !ack.RequestCPUConfig {
		t.Fatal("a node the scheduler holds no CPU config for was not asked for one")
	}

	_, ack, err = registry.Heartbeat(cpuConfigHeartbeat("node-a", `{"cpuid_modifiers":[]}`), now.Add(time.Second))
	if err != nil {
		t.Fatalf("heartbeat with config: %v", err)
	}
	if ack.RequestCPUConfig {
		t.Fatal("the scheduler asked again for a CPU config it had just been given")
	}
}

// The node stops sending its configuration after the first accepted heartbeat,
// so every heartbeat after it carries none. The registry carries the stored one
// forward onto each new record; without that the ask would come back on the
// heartbeat after the answer, and the intersection would be invalidated and
// recomputed forever.
func TestHeartbeatCarriesAStoredCPUConfigForward(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)

	if _, _, err := registry.Heartbeat(cpuConfigHeartbeat("node-a", `{"cpuid_modifiers":[]}`), now); err != nil {
		t.Fatalf("heartbeat with config: %v", err)
	}

	_, ack, err := registry.Heartbeat(cpuConfigHeartbeat("node-a", ""), now.Add(time.Second))
	if err != nil {
		t.Fatalf("heartbeat without config: %v", err)
	}
	if ack.RequestCPUConfig {
		t.Fatal("a heartbeat carrying no config dropped the stored one and asked for it again")
	}

	observed, ok := registry.GetObserved("node-a", "cluster", now.Add(time.Second))
	if !ok {
		t.Fatal("expected an observed record")
	}
	if got := observed.GetMachineInfo().GetCpuConfigJson(); got != `{"cpuid_modifiers":[]}` {
		t.Fatalf("stored cpu config = %q, want the one the node sent", got)
	}
}

// The defect this exists for: the scheduler holds the intersection in memory
// only, so a restart leaves it with nothing, and a node that has already sent
// its configuration has no reason to send it again. The restarted scheduler has
// to ask.
func TestRestartedSchedulerAsksForTheCPUConfigAgain(t *testing.T) {
	nodes := []Node{{ID: "node-a", Endpoint: "http://node-a"}}
	first := NewAtomicNodeRegistry(nodes, 30*time.Second)
	now := time.Unix(100, 0)
	if _, _, err := first.Heartbeat(cpuConfigHeartbeat("node-a", `{"cpuid_modifiers":[]}`), now); err != nil {
		t.Fatalf("heartbeat with config: %v", err)
	}

	restarted := NewAtomicNodeRegistry(nodes, 30*time.Second)
	_, ack, err := restarted.Heartbeat(cpuConfigHeartbeat("node-a", ""), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("heartbeat after restart: %v", err)
	}
	if !ack.RequestCPUConfig {
		t.Fatal("a scheduler that restarted did not ask for the CPU config it no longer holds")
	}
}

// The ask has to reach the node, which means it has to be on the response the
// RPC returns and not only in the registry's answer to the service.
func TestHeartbeatResponseCarriesTheCPUConfigRequest(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	service := NewService(nil, registry, NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute))

	resp, err := service.Heartbeat(context.Background(), cpuConfigHeartbeat("node-a", ""))
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !resp.GetRequestCpuConfig() {
		t.Fatal("HeartbeatResponse did not ask for the CPU config the scheduler is missing")
	}

	resp, err = service.Heartbeat(context.Background(), cpuConfigHeartbeat("node-a", `{"cpuid_modifiers":[]}`))
	if err != nil {
		t.Fatalf("heartbeat with config: %v", err)
	}
	if resp.GetRequestCpuConfig() {
		t.Fatal("HeartbeatResponse kept asking for a CPU config the scheduler already holds")
	}
}
