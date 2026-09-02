package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
)

// A draining node is what an operator is watching most closely, and the
// aggregated list is where they watch it. It has to be named in the API's own
// vocabulary or a spec-generated client rejects the whole list.
func TestNodeListRendersADrainingNode(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		listObservedFunc: func(_ context.Context, _ *schedulerv1.ListObservedNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			return &schedulerv1.ListObservedNodesResponse{
				Nodes: []*schedulerv1.ObservedNode{{
					NodeId:   "node-draining",
					Snapshot: &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_LINGERING},
				}},
			}, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/nodes", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	var nodes []struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &nodes); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Status != "draining" {
		t.Fatalf("rendered %+v, want one node with status draining", nodes)
	}
}

// Every status the scheduler can report must map into the enum the HTTP API
// promises (src/api/openapi.yml NodeStatus). Walking the proto's own name table
// means the next status added there fails here rather than in a client.
func TestEveryNodeStatusRendersInsideTheAPIEnum(t *testing.T) {
	apiEnum := map[string]bool{"ready": true, "draining": true, "connecting": true, "unhealthy": true}
	for value, name := range schedulerv1.NodeStatus_name {
		status := schedulerv1.NodeStatus(value)
		if status == schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED {
			continue
		}
		if got := nodeStatusToString(status); !apiEnum[got] {
			t.Errorf("%s renders as %q, which is outside the API's NodeStatus enum", name, got)
		}
	}
}
