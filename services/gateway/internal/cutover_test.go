package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A sandbox that moves between the lookup and the request must not fail with
// the old node's 404, which reads to a client as "your sandbox is gone" rather
// than "it is somewhere else now".
func TestARequestFollowsASandboxToItsNewNode(t *testing.T) {
	var oldHits, newHits atomic.Int32

	oldNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		oldHits.Add(1)
		w.Header().Set(headerSandboxDisowned, "1")
		http.Error(w, "sandbox not found", http.StatusNotFound)
	}))
	defer oldNode.Close()

	newNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newHits.Add(1)
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(append([]byte("served:"), body...))
	}))
	defer newNode.Close()

	sandboxID := "00000000-0000-7000-8000-000000000001"
	scheduler := &movingSchedulerClient{
		first:  &fakeNode{id: "node-old", endpoint: oldNode.URL},
		second: &fakeNode{id: "node-new", endpoint: newNode.URL},
	}
	server := newCutoverTestServer(t, scheduler)

	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("payload"))
	request.Header.Set(headerSandboxID, sandboxID)
	request.Header.Set(headerTargetPort, "8000")
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected the retry to succeed, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Body.String(); got != "served:payload" {
		t.Fatalf("the body must be replayed intact, got %q", got)
	}
	if oldHits.Load() != 1 || newHits.Load() != 1 {
		t.Fatalf("expected one attempt each, got old=%d new=%d", oldHits.Load(), newHits.Load())
	}
	if scheduler.lookups.Load() < 2 {
		t.Fatalf("the disown should have forced a re-resolve, got %d lookups", scheduler.lookups.Load())
	}
}

// The guest's own application returns 404 constantly. Treating those as a
// moved sandbox would cost a scheduler round trip on every one.
func TestAnApplicationNotFoundDoesNotReResolve(t *testing.T) {
	var hits atomic.Int32
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		http.Error(w, "no such route in my app", http.StatusNotFound)
	}))
	defer node.Close()

	scheduler := &movingSchedulerClient{
		first:  &fakeNode{id: "node-a", endpoint: node.URL},
		second: &fakeNode{id: "node-a", endpoint: node.URL},
	}
	server := newCutoverTestServer(t, scheduler)

	request := httptest.NewRequest(http.MethodGet, "/missing", nil)
	request.Header.Set(headerSandboxID, "00000000-0000-7000-8000-000000000002")
	request.Header.Set(headerTargetPort, "8000")
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("the application's 404 must reach the client, got %d", recorder.Code)
	}
	if hits.Load() != 1 {
		t.Fatalf("expected exactly one upstream attempt, got %d", hits.Load())
	}
	if scheduler.lookups.Load() != 1 {
		t.Fatalf("an application 404 must not re-resolve, got %d lookups", scheduler.lookups.Load())
	}
}

// A disown plus no new owner is what a genuinely deleted sandbox looks like.
// The node's own answer is the truthful one and must reach the client.
func TestADeletedSandboxSurfacesTheNodesAnswer(t *testing.T) {
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerSandboxDisowned, "1")
		http.Error(w, "sandbox not found", http.StatusNotFound)
	}))
	defer node.Close()

	scheduler := &movingSchedulerClient{
		first:  &fakeNode{id: "node-a", endpoint: node.URL},
		second: nil, // gone
	}
	server := newCutoverTestServer(t, scheduler)

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(headerSandboxID, "00000000-0000-7000-8000-000000000003")
	request.Header.Set(headerTargetPort, "8000")
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected the node's 404, got %d", recorder.Code)
	}
}

// Two nodes that each disown the sandbox must not send one request round in
// circles.
func TestACutoverIsBounded(t *testing.T) {
	var hits atomic.Int32
	disowning := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.Header().Set(headerSandboxDisowned, "1")
			http.Error(w, "sandbox not found", http.StatusNotFound)
		}))
	}
	first, second := disowning(), disowning()
	defer first.Close()
	defer second.Close()

	scheduler := &movingSchedulerClient{
		first:  &fakeNode{id: "node-a", endpoint: first.URL},
		second: &fakeNode{id: "node-b", endpoint: second.URL},
	}
	server := newCutoverTestServer(t, scheduler)

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(headerSandboxID, "00000000-0000-7000-8000-000000000004")
	request.Header.Set(headerTargetPort, "8000")
	server.ServeHTTP(httptest.NewRecorder(), request)

	if hits.Load() > maxCutoverRetries+1 {
		t.Fatalf("a request must not chase a sandbox indefinitely, got %d attempts", hits.Load())
	}
}

// fakeNode is a scheduler node answer.
type fakeNode struct {
	id       string
	endpoint string
}

// movingSchedulerClient answers the first lookup with one node and every later
// one with another, which is what a sandbox migrating mid-request looks like
// from the gateway.
type movingSchedulerClient struct {
	stubSchedulerClient
	first   *fakeNode
	second  *fakeNode
	lookups atomic.Int32
}

func (c *movingSchedulerClient) LookupNode(
	_ context.Context,
	_ *schedulerv1.LookupNodeRequest,
	_ ...grpc.CallOption,
) (*schedulerv1.LookupNodeResponse, error) {
	answer := c.first
	if c.lookups.Add(1) > 1 {
		answer = c.second
	}
	if answer == nil {
		return nil, status.Error(codes.NotFound, "sandbox binding not found")
	}
	return &schedulerv1.LookupNodeResponse{
		Node: &schedulerv1.Node{NodeId: answer.id, Endpoint: answer.endpoint},
	}, nil
}

func newCutoverTestServer(t *testing.T, client schedulerv1.SchedulerClient) http.Handler {
	t.Helper()
	// The binding cache is disabled so each test exercises the disown path
	// rather than whichever entry a previous lookup happened to leave behind.
	server := newTestServer(t, client, 5*time.Second, 1<<20)
	return authenticatedTestHandler(server)
}
