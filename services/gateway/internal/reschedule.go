package gateway

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
)

// maxScheduleAttempts bounds how many nodes one create may be offered to.
//
// A node's admission decision is authoritative over the scheduler's stale
// capacity view, so a rejection has to be retried elsewhere or the rejection
// is just a user-visible failure. The bound matters as much as the retry: with
// a full fleet every create would otherwise walk the entire node list,
// multiplying scheduler load exactly when the fleet can least absorb it.
const maxScheduleAttempts = 3

// bufferedResponse captures an upstream response so a retryable rejection can
// be discarded instead of reaching the client.
//
// Only used for scheduled creates, which are small non-streaming responses.
// Streaming and long-lived requests never take this path, so nothing that
// needs incremental flushing is buffered here.
type bufferedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newBufferedResponse() *bufferedResponse {
	return &bufferedResponse{header: make(http.Header), status: http.StatusOK}
}

func (b *bufferedResponse) Header() http.Header { return b.header }

func (b *bufferedResponse) WriteHeader(status int) {
	if b.status != http.StatusOK {
		return
	}
	b.status = status
}

func (b *bufferedResponse) Write(p []byte) (int, error) {
	return b.body.Write(p)
}

// replay writes the captured response to the real client.
func (b *bufferedResponse) replay(w http.ResponseWriter) {
	dst := w.Header()
	for key, values := range b.header {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
	// Content-Length may have been set from a body we are replaying verbatim,
	// so recompute rather than trusting a stale value.
	if dst.Get("Content-Length") != "" {
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
// A 503 from a node on the create path is always that: either the node is
// shutting down or its admission gate refused. Both are answers about that
// node, not about the request, and both are resolved by placing elsewhere.
// Every other status is the node's real answer and is returned to the client.
func retryableRejection(status int) bool {
	return status == http.StatusServiceUnavailable
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
