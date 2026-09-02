package gateway

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
)

// defaultMaxScheduleRetries bounds how many further nodes one create may be
// offered to after the first refuses it.
//
// A node's admission decision is authoritative over the scheduler's stale
// capacity view, so a rejection has to be retried elsewhere or the rejection
// is just a user-visible failure. The bound matters as much as the retry: with
// a full fleet every create would otherwise walk the entire node list,
// multiplying scheduler load exactly when the fleet can least absorb it.
const defaultMaxScheduleRetries = 2

// scheduleRetryBound resolves the configured retry count.
//
// Zero is what an unset option looks like, so it takes the default rather
// than silently turning retries off; disabling has to be something someone
// chose, which is what a negative value says.
func scheduleRetryBound(configured int) int {
	switch {
	case configured < 0:
		return 0
	case configured == 0:
		return defaultMaxScheduleRetries
	default:
		return configured
	}
}

// bufferedResponse captures an upstream response so a retryable rejection or a
// disown can be discarded instead of reaching the client.
//
// A bounded buffer spills rather than drops. Once the body outgrows the limit,
// the captured status, headers and prefix are committed to the spill writer and
// every later byte streams straight through. From that point the response is
// the client's and cannot be discarded — but the alternative, handing the
// request back to the direct path, executed it upstream a second time, and a
// POST that had already run once ran again. A response too large to hold is
// delivered once, not re-earned.
type bufferedResponse struct {
	header http.Header
	status int
	// wroteHeader distinguishes "nobody set a status" from "somebody set 200".
	// Comparing against http.StatusOK cannot: a handler that legitimately
	// writes 200 would then be overwritten by whatever came next.
	wroteHeader bool
	body        bytes.Buffer
	// limit bounds what is held in memory. Zero means unbounded.
	limit int64
	// spill receives the response once it outgrows limit.
	spill http.ResponseWriter
	// spilled records that the response has been committed to spill, so the
	// caller knows it can no longer be discarded or replayed.
	spilled bool
}

func newBufferedResponse() *bufferedResponse {
	return &bufferedResponse{header: make(http.Header), status: http.StatusOK}
}

// newBoundedBufferedResponse caps what is held in memory; past the cap the
// response is committed to spill and streamed.
func newBoundedBufferedResponse(limit int64, spill http.ResponseWriter) *bufferedResponse {
	return &bufferedResponse{
		header: make(http.Header),
		status: http.StatusOK,
		limit:  limit,
		spill:  spill,
	}
}

// Header hands out the spill writer's headers once committed, so trailers the
// proxy adds after the body land on the response the client is reading.
func (b *bufferedResponse) Header() http.Header {
	if b.spilled {
		return b.spill.Header()
	}
	return b.header
}

func (b *bufferedResponse) WriteHeader(status int) {
	// 1xx is informational: ReverseProxy forwards Early Hints by calling
	// WriteHeader with a 1xx and then again with the real status. Latching the
	// first one would report 103 as the terminal status and drop the response
	// the client was actually given.
	if status >= 100 && status < 200 {
		return
	}
	if b.wroteHeader {
		return
	}
	b.wroteHeader = true
	b.status = status
}

func (b *bufferedResponse) Write(p []byte) (int, error) {
	if b.spilled {
		return b.spill.Write(p)
	}
	// This is the default path for every ordinary proxied request, so without
	// a ceiling one large upstream body is a memory vector. Crossing it commits
	// what has been captured and streams the rest; the upstream has already
	// done the work, and nothing may ask it to do that work again.
	if b.limit > 0 && int64(b.body.Len())+int64(len(p)) > b.limit {
		b.spilled = true
		b.commit(b.spill, false)
		return b.spill.Write(p)
	}
	return b.body.Write(p)
}

// Flush only means something once the response has been committed; before
// that there is nothing downstream to flush to.
func (b *bufferedResponse) Flush() {
	if !b.spilled {
		return
	}
	if flusher, ok := b.spill.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (b *bufferedResponse) statusCode() int { return b.status }

func (b *bufferedResponse) statusWritten() bool { return b.wroteHeader }

// replay writes the captured response to the real client.
func (b *bufferedResponse) replay(w http.ResponseWriter) {
	// Content-Length may have been set from a body we are replaying verbatim,
	// so recompute rather than trusting a stale value.
	b.commit(w, true)
}

// commit writes the captured status, headers and body so far to w. The
// upstream's Content-Length is kept when the body is about to continue past
// what was captured: it describes the whole stream, and the prefix does not.
func (b *bufferedResponse) commit(w http.ResponseWriter, recomputeLength bool) {
	dst := w.Header()
	for key, values := range b.header {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
	if recomputeLength && dst.Get("Content-Length") != "" {
		dst.Set("Content-Length", strconv.Itoa(b.body.Len()))
	}
	w.WriteHeader(b.status)
	if b.body.Len() > 0 {
		_, _ = w.Write(b.body.Bytes())
	}
}

// retryableRejection reports whether a create response means "this node cannot
// take it, try another".
//
// Only a refusal the node names as capacity is that: 503 carrying
// x-agentenv-refusal-reason: node_at_capacity. A bare 503 is not enough. It is
// also what a node mid-fault, a proxy in front of it, or an older node with a
// different idea of the status code says, and none of those is cured by
// placing elsewhere. The node knows which it is and says so; the gateway does
// not guess. Every other answer is the node's real answer about the request
// and reaches the client unchanged.
func retryableRejection(status int, header http.Header) bool {
	return status == http.StatusServiceUnavailable &&
		header.Get(headerRefusalReason) == refusalNodeAtCapacity
}

// replayableRequestBody buffers the request body so a second placement attempt
// can send it again, and reports whether that succeeded.
//
// Bodies above the hint budget are left as a one-shot stream: retrying with a
// partially consumed stream would send a truncated create, so those get a
// single attempt. The prefix already read is stitched back in front of the
// remainder either way, so the upstream request is always complete.
func replayableRequestBody(r *http.Request) ([]byte, bool) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, true
	}
	orig := r.Body
	buf, err := io.ReadAll(io.LimitReader(orig, maxHintBodyBytes+1))
	if err != nil {
		r.Body = &prefixedBody{Reader: io.MultiReader(bytes.NewReader(buf), orig), closer: orig}
		return nil, false
	}
	if int64(len(buf)) > maxHintBodyBytes {
		r.Body = &prefixedBody{Reader: io.MultiReader(bytes.NewReader(buf), orig), closer: orig}
		return nil, false
	}
	_ = orig.Close()
	r.Body = io.NopCloser(bytes.NewReader(buf))
	r.ContentLength = int64(len(buf))
	return buf, true
}

// setReplayableBody gives a retry attempt its own reader over the buffered body.
func setReplayableBody(r *http.Request, body []byte) {
	if body == nil {
		r.Body = http.NoBody
		r.ContentLength = 0
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
}
