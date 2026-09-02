package gateway

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"agentenv/services/shared/observability"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	gatewayCreateRefusalsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_gateway_create_refusals_total",
			Help: "Creates refused by the gateway, by reason.",
		},
		[]string{"reason"},
	)
	gatewayScheduleRetriesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "agentenv_gateway_schedule_retries_total",
			Help: "Creates re-placed after a node refused them.",
		},
	)
	gatewayHTTPDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_gateway_http_request_duration_seconds",
			Help:    "Gateway HTTP request duration by method, route, route source, and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"method", "route", "route_source", "status"},
	)
	gatewayUpstreamProxyDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_gateway_upstream_proxy_duration_seconds",
			Help:    "Gateway upstream reverse proxy duration by route and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"route", "status"},
	)
	gatewaySchedulerRPCDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_gateway_scheduler_rpc_duration_seconds",
			Help:    "Gateway scheduler RPC duration by RPC and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"rpc", "status"},
	)
	// result is a closed set: hit, miss, negative_hit, evict. Read beside the
	// LookupNode RPC rate — misses that coalesced onto one round trip show up
	// as the gap between the two.
	gatewayBindingCacheTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_gateway_binding_cache_total",
			Help: "Sandbox binding cache outcomes, by result.",
		},
		[]string{"result"},
	)
)

func recordGatewayBindingCache(result string) {
	gatewayBindingCacheTotal.WithLabelValues(result).Inc()
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	routeSource routeSource
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(body)
}

func (r *statusRecorder) Flush() {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijack")
	}
	conn, brw, err := hijacker.Hijack()
	if err == nil && r.status == 0 {
		r.status = http.StatusSwitchingProtocols
	}
	return conn, brw, err
}

func (r *statusRecorder) statusCode() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

func (r *statusRecorder) statusWritten() bool { return r.status != 0 }

func (r *statusRecorder) setRouteSource(source routeSource) {
	r.routeSource = source
}

func (r *statusRecorder) routeSourceLabel() string {
	if r.routeSource == "" {
		return "unknown"
	}
	return string(r.routeSource)
}

type routeSourceRecorder interface {
	setRouteSource(routeSource)
}

func setGatewayRouteSource(w http.ResponseWriter, source routeSource) {
	if recorder, ok := w.(routeSourceRecorder); ok {
		recorder.setRouteSource(source)
	}
}

func (s *Server) instrumentGatewayHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.isLocalGatewayEndpointRequest(r) {
			next.ServeHTTP(w, r)
			return
		}
		// Sandbox-routed /health and /metrics requests are user traffic, so
		// keep them instrumented like any other proxy call.

		method := gatewayMethodLabel(r.Method)
		route := gatewayRouteLabel(r.URL.Path)
		recorder := &statusRecorder{ResponseWriter: w}
		start := time.Now()
		next.ServeHTTP(recorder, r)
		status := httpStatusLabel(recorder, r.Context())

		gatewayHTTPDuration.WithLabelValues(method, route, recorder.routeSourceLabel(), status).Observe(time.Since(start).Seconds())
	})
}

func (s *Server) isLocalGatewayEndpointRequest(r *http.Request) bool {
	if (r.URL.Path != "/health" && r.URL.Path != "/metrics") || hasProxyRoutingHeaders(r.Header) {
		return false
	}
	hostRoute, hostRouteErr := parseHostRoute(r.Host, s.sandboxProxyDomains)
	return hostRoute == nil && hostRouteErr == nil
}

func recordGatewaySchedulerRPC(rpc string, start time.Time, err error) {
	status := observability.GRPCStatusLabel(err)
	gatewaySchedulerRPCDuration.WithLabelValues(rpc, status).Observe(time.Since(start).Seconds())
}

func recordGatewayUpstreamProxy(route string, start time.Time, w http.ResponseWriter, ctx context.Context) {
	gatewayUpstreamProxyDuration.WithLabelValues(route, httpStatusLabel(w, ctx)).Observe(time.Since(start).Seconds())
}

func gatewayMethodLabel(method string) string {
	switch method {
	case http.MethodGet:
		return "GET"
	case http.MethodPost:
		return "POST"
	case http.MethodPut:
		return "PUT"
	case http.MethodPatch:
		return "PATCH"
	case http.MethodDelete:
		return "DELETE"
	case http.MethodHead:
		return "HEAD"
	case http.MethodOptions:
		return "OPTIONS"
	default:
		return "OTHER"
	}
}

// statusReporter is what a response writer has to expose for the metrics to
// label it honestly. The buffered writers on the reschedule and cutover paths
// implement it too: an attempt a node refused is a 5xx, not "other", and the
// difference is the whole point of the upstream duration metric.
type statusReporter interface {
	statusCode() int
	statusWritten() bool
}

// httpStatusLabel derives the status bucket for a recorded response. ctx must
// be the inbound request context (r.Context()): its cancellation means the
// client disconnected, not that the gateway timed out (an upstream timeout is
// context.DeadlineExceeded and is turned into a 504 by the proxy ErrorHandler).
// So when no status was written and ctx was cancelled, it reports
// "client_closed" (nginx-style 499) instead of counting the request as 2xx.
func httpStatusLabel(w http.ResponseWriter, ctx context.Context) string {
	reporter, ok := w.(statusReporter)
	if !ok {
		return "other"
	}
	if !reporter.statusWritten() && errors.Is(ctx.Err(), context.Canceled) {
		return "client_closed"
	}
	status := reporter.statusCode()
	switch {
	case status >= 100 && status < 200:
		return "1xx"
	case status >= 200 && status < 300:
		return "2xx"
	case status >= 300 && status < 400:
		return "3xx"
	case status >= 400 && status < 500:
		return "4xx"
	case status >= 500 && status < 600:
		return "5xx"
	default:
		return "other"
	}
}

func gatewayRouteLabel(path string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(path), "/")
	if trimmed == "" {
		trimmed = "/"
	}

	switch trimmed {
	case "/sandboxes", "/sandboxes-cold", "/v2/sandboxes", "/nodes":
		return trimmed
	}

	parts := strings.Split(strings.Trim(trimmed, "/"), "/")
	if len(parts) == 0 {
		return "unmatched"
	}
	switch parts[0] {
	case "sandboxes":
		if len(parts) == 2 {
			return "/sandboxes/{sandbox_id}"
		}
		if len(parts) == 3 {
			switch parts[2] {
			case "snapshots", "custom-extension-params", "pause", "resume", "fork":
				return "/sandboxes/{sandbox_id}/" + parts[2]
			}
		}
	case "nodes":
		if len(parts) == 2 {
			return "/nodes/{node_id}"
		}
	case "proxy":
		return "/proxy/*"
	}
	return "unmatched"
}

// recordGatewayScheduleRetry counts creates a node refused and the gateway
// re-placed. A non-zero rate is the admission gate working; a rate close to
// the create rate means the fleet is saturated rather than merely unbalanced.
// Both signals are rates of the whole, so the counter carries no labels: a
// per-node label would grow one series per refusing node per gateway, fastest
// during exactly the capacity incident it is meant to explain. Which node
// refused is in the debug log beside the retry.
func recordGatewayScheduleRetry() {
	gatewayScheduleRetriesTotal.Inc()
}

// recordGatewayRefusal counts creates the gateway refused, by reason. The
// reasons are a closed set, and separating them matters: shedding means the
// gateway is the constraint, exhaustion means the fleet is.
func recordGatewayRefusal(reason string) {
	gatewayCreateRefusalsTotal.WithLabelValues(reason).Inc()
}

// gatewayCutoverFollowedTotal counts requests that were re-routed after the
// node they were sent to disowned the sandbox. A rising rate without
// migrations happening means bindings are going stale for some other reason.
var gatewayCutoverFollowedTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "agentenv_gateway_sandbox_cutover_followed_total",
		Help: "Requests re-routed to a new node after the previous owner disowned the sandbox.",
	},
)
