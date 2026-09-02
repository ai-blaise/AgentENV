package gateway

import (
	"context"
	"net/http"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"go.uber.org/zap"
)

// headerSandboxDisowned is set by a node that does not have the sandbox a
// request was routed to.
//
// A bare 404 cannot carry that meaning: the guest's own application returns
// 404 constantly, and treating those as a moved sandbox would re-resolve the
// binding on every one. The node knows the difference and says so.
const headerSandboxDisowned = "x-agentenv-sandbox-disowned"

// maxCutoverRetries bounds how many times one request chases a moving sandbox.
//
// One is enough for a cutover, which has a single old owner and a single new
// one. More would let a request loop between two nodes that each disown it —
// which is what a genuinely deleted sandbox looks like from here.
const maxCutoverRetries = 1

func isSandboxDisowned(resp *http.Response) bool {
	return resp != nil && resp.Header.Get(headerSandboxDisowned) != ""
}

// resolveAfterCutover re-resolves a sandbox whose owner just disowned it.
//
// Returns the new owner, or false when the sandbox is genuinely gone — a
// disown plus no binding is the honest description of a deleted sandbox, and
// the caller should let the node's own 404 through.
func (s *Server) resolveAfterCutover(
	ctx context.Context,
	sandboxID string,
	previous *schedulerv1.Node,
) (*schedulerv1.Node, bool) {
	if sandboxID == "" {
		return nil, false
	}
	// The cached entry is what sent the request to the wrong node, so it has
	// to go before the lookup rather than after.
	s.bindingCache.Invalidate(sandboxID)

	rpcStart := time.Now()
	resp, err := s.queryOnlyScheduler.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
	recordGatewaySchedulerRPC("LookupNode", rpcStart, err)
	if err != nil {
		s.logger.Debug("gateway could not re-resolve a disowned sandbox",
			zap.String("sandbox_id", sandboxID),
			zap.Error(err),
		)
		return nil, false
	}
	node := resp.GetNode()
	if node.GetEndpoint() == "" {
		return nil, false
	}
	// The same node again means the disown and the binding disagree. Retrying
	// would just produce the same answer, more slowly.
	if node.GetNodeId() == previous.GetNodeId() {
		s.logger.Debug("gateway re-resolved a disowned sandbox to the same node",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", node.GetNodeId()),
		)
		return nil, false
	}
	return node, true
}

// proxyWithCutover handles a request whose sandbox may have just moved.
//
// The request is proxied into a buffer so a disown can be discarded instead of
// reaching the client. That buffering is why this is not the path for every
// request: a streaming or long-lived response must not be held, and a large
// upload cannot be replayed. Those keep the direct path, where a disown still
// invalidates the cache — the next request lands correctly, this one does not.
//
// The decision to decline is made before the upstream is asked. Once a request
// has been issued it is never issued again by anyone: a response that outgrows
// the buffer is streamed to the client from where it stands, forfeiting the
// cutover for that one response rather than re-running work the node has
// already done.
//
// Reports whether it handled the request.
func (s *Server) proxyWithCutover(
	w http.ResponseWriter,
	r *http.Request,
	routingCtx context.Context,
	upstreamCtx context.Context,
	node *schedulerv1.Node,
	options proxyRequestOptions,
	sandboxID string,
	routeSource routeSource,
	longLived bool,
) bool {
	if sandboxID == "" || longLived || options.flushImmediately {
		return false
	}
	body, replayable := replayableRequestBody(r)
	if !replayable {
		return false
	}

	current := node
	for attempt := 0; ; attempt++ {
		upstreamURL, err := joinUpstream(
			current.GetEndpoint(),
			upstreamTargetPath(routeSource, r.URL.Path),
			upstreamTargetEscapedPath(routeSource, requestEscapedPath(r)),
			r.URL.RawQuery,
		)
		if err != nil {
			http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
			return true
		}

		attemptReq := r.Clone(upstreamCtx)
		setReplayableBody(attemptReq, body)

		// Bounded: this is the default path for every ordinary sandbox
		// request, and an unbounded buffer here means one large upstream
		// response is a memory vector. Past the bound the response spills to
		// the client rather than being dropped.
		buffered := newBoundedBufferedResponse(s.maxRespSize, w)
		disowned := false
		attemptOptions := options
		attemptOptions.onDisown = func() { disowned = true }
		s.proxyRequest(buffered, attemptReq, r.Context(), upstreamURL, current, attemptOptions)

		if buffered.spilled {
			// Already the client's. There is nothing left to decide, and in
			// particular nothing to hand back to the direct path: the upstream
			// has executed this request once, which is the only time it may.
			return true
		}
		if !disowned || attempt >= maxCutoverRetries {
			buffered.replay(w)
			return true
		}

		next, ok := s.resolveAfterCutover(routingCtx, sandboxID, current)
		if !ok {
			// Nowhere else to try. The node's own answer is the truthful one.
			buffered.replay(w)
			return true
		}
		s.logger.Debug("gateway followed a sandbox to its new node",
			zap.String("sandbox_id", sandboxID),
			zap.String("from_node_id", current.GetNodeId()),
			zap.String("to_node_id", next.GetNodeId()),
		)
		gatewayCutoverFollowedTotal.Inc()
		current = next
	}
}
