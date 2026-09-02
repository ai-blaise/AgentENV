package gateway

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
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

// The cutover path is the default for every ordinary sandbox request, and it
// holds the whole response in memory. Without a ceiling one large upstream
// body is a memory vector on the busiest path in the process. The ceiling is
// set well below the body here so the overflow is actually exercised.
func TestALargeResponseIsNotHeldInMemoryByTheCutoverPath(t *testing.T) {
	const limit = 64 * 1024
	body := bytes.Repeat([]byte("x"), limit*4)

	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer upstream.Close()

	var lookups atomic.Int32
	client := &countingLookupClient{endpoint: upstream.URL, lookups: &lookups}
	server := newTestServer(t, client, 5*time.Second, limit, func(o *ServerOptions) {
		o.BindingCache.TTL = time.Minute
	})
	handler := authenticatedTestHandler(server)

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(headerSandboxID, "00000000-0000-7000-8000-0000000000cc")
	request.Header.Set(headerTargetPort, "8000")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	// The oversized response must still reach the client intact — the bound
	// changes how it is carried, not whether it is delivered.
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected the response to be delivered, got %d", recorder.Code)
	}
	if recorder.Body.Len() != len(body) {
		t.Fatalf("body truncated: got %d bytes, want %d", recorder.Body.Len(), len(body))
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream executed %d times, want 1", got)
	}
}

// A response that outgrows the cutover buffer used to be handed back to the
// direct path, which asked the upstream again — so a POST whose output was
// large ran twice, and the client learned only the second outcome. The
// upstream is asked once; what it says is streamed from where the buffer left
// off, headers and status as captured.
func TestAnOverflowingResponseIsExecutedUpstreamExactlyOnce(t *testing.T) {
	const limit = 64 * 1024
	output := bytes.Repeat([]byte("y"), limit*3)

	var hits atomic.Int32
	var bodies []string
	var mu sync.Mutex
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		received, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(received))
		mu.Unlock()
		w.Header().Set("X-Upstream-Marker", "ran")
		w.Header().Set("Content-Length", strconv.Itoa(len(output)))
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write(output)
	}))
	defer upstream.Close()

	var lookups atomic.Int32
	client := &countingLookupClient{endpoint: upstream.URL, lookups: &lookups}
	server := newTestServer(t, client, 5*time.Second, limit)
	handler := authenticatedTestHandler(server)

	request := httptest.NewRequest(http.MethodPost, "/append", strings.NewReader("append-one-line"))
	request.Header.Set(headerSandboxID, "00000000-0000-7000-8000-0000000000dd")
	request.Header.Set(headerTargetPort, "8000")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	mu.Lock()
	defer mu.Unlock()
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream executed %d times, want 1; bodies received per execution: %q", got, bodies)
	}
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want the upstream's 202 as captured", recorder.Code)
	}
	if got := recorder.Header().Get("X-Upstream-Marker"); got != "ran" {
		t.Fatalf("captured headers must be committed with the spill, got marker %q", got)
	}
	// The upstream's Content-Length describes the whole stream, and the whole
	// stream is what the client gets; it must not be rewritten to the prefix.
	if got := recorder.Header().Get("Content-Length"); got != strconv.Itoa(len(output)) {
		t.Fatalf("Content-Length = %q, want %d", got, len(output))
	}
	if !bytes.Equal(recorder.Body.Bytes(), output) {
		t.Fatalf("body delivered %d bytes, want the upstream's %d intact", recorder.Body.Len(), len(output))
	}
}

// Spilling commits exactly once — status, headers, then the held prefix — and
// everything after streams through, including headers the proxy adds late.
func TestASpilledResponseCommitsOnceAndStreamsTheRest(t *testing.T) {
	sink := httptest.NewRecorder()
	buffered := newBoundedBufferedResponse(8, sink)
	buffered.Header().Set("X-Captured", "1")
	buffered.WriteHeader(http.StatusCreated)

	if _, err := buffered.Write([]byte("12345")); err != nil {
		t.Fatal(err)
	}
	if buffered.spilled || sink.Body.Len() != 0 {
		t.Fatal("a body within the limit must stay buffered")
	}
	if _, err := buffered.Write([]byte("6789")); err != nil {
		t.Fatal(err)
	}
	if !buffered.spilled {
		t.Fatal("crossing the limit must spill")
	}
	if _, err := buffered.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	buffered.Header().Set("X-Late", "trailer")
	buffered.Flush()

	if sink.Code != http.StatusCreated {
		t.Fatalf("committed status = %d, want 201", sink.Code)
	}
	if sink.Header().Get("X-Captured") != "1" {
		t.Fatal("captured headers must be committed with the spill")
	}
	if got := sink.Body.String(); got != "123456789tail" {
		t.Fatalf("body = %q, want the prefix then the rest in order", got)
	}
	if sink.Header().Get("X-Late") != "trailer" {
		t.Fatal("headers added after the spill must land on the real writer")
	}
	if !sink.Flushed {
		t.Fatal("a flush after the spill must reach the real writer")
	}
}

// A disown on the direct path must invalidate the cached binding no matter how
// the request named its sandbox. A host-routed request carries the id only in
// the Host label, so the response side cannot recover it from headers and has
// to be told.
func TestADirectPathDisownInvalidatesTheBindingHoweverItWasRouted(t *testing.T) {
	const domain = "sbx.example.com"
	for _, tc := range []struct {
		name    string
		prepare func(r *http.Request)
	}{
		{
			name: "header-routed",
			prepare: func(r *http.Request) {
				r.Header.Set(headerSandboxID, "sandboxa")
				r.Header.Set(headerTargetPort, "8000")
			},
		},
		{
			name: "host-routed",
			prepare: func(r *http.Request) {
				r.Host = "8000-sandboxa." + domain
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(headerSandboxDisowned, "1")
				http.Error(w, "sandbox not found", http.StatusNotFound)
			}))
			defer upstream.Close()

			var lookups atomic.Int32
			client := &countingLookupClient{endpoint: upstream.URL, lookups: &lookups}
			server := newTestServer(t, client, 5*time.Second, 1<<20,
				withSandboxProxyDomains(domain),
				func(o *ServerOptions) { o.BindingCache.TTL = time.Minute },
			)
			handler := authenticatedTestHandler(server)

			// Server-sent events are long-lived, which keeps them off the
			// buffered cutover path and on the direct one.
			for i := 0; i < 2; i++ {
				request := httptest.NewRequest(http.MethodGet, "/events", nil)
				request.Header.Set("Accept", "text/event-stream")
				tc.prepare(request)
				handler.ServeHTTP(httptest.NewRecorder(), request)
			}

			if got := lookups.Load(); got != 2 {
				t.Fatalf("underlying lookups = %d, want 2: the disown must have dropped the cached binding", got)
			}
		})
	}
}

// ReverseProxy forwards Early Hints by calling WriteHeader with a 1xx and then
// again with the real status. Latching the first would report 103 as the
// terminal status and drop the one the client was actually given.
func TestAnInformationalStatusDoesNotLatch(t *testing.T) {
	buffered := newBufferedResponse()
	buffered.WriteHeader(http.StatusEarlyHints)
	buffered.WriteHeader(http.StatusCreated)

	if buffered.status != http.StatusCreated {
		t.Fatalf("terminal status = %d, want %d", buffered.status, http.StatusCreated)
	}

	// And a genuine 200 must not be overwritten by anything that follows.
	second := newBufferedResponse()
	second.WriteHeader(http.StatusOK)
	second.WriteHeader(http.StatusInternalServerError)
	if second.status != http.StatusOK {
		t.Fatalf("first terminal status should win, got %d", second.status)
	}
}
