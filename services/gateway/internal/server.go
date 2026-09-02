package gateway

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	headerAPIKey               = "X-API-Key"
	headerTrafficToken         = "e2b-traffic-access-token"
	headerEnvdAccessToken      = "X-Access-Token"
	headerSandboxID            = "x-agentenv-sandbox-id"
	headerE2BSandboxID         = "e2b-sandbox-id"
	headerTargetPort           = "x-agentenv-target-port"
	headerE2BTargetPort        = "e2b-sandbox-port"
	headerNodeID               = "x-agentenv-node-id"
	maxRecordAssignmentTimeout = 5 * time.Second
)

type routeSource string

const (
	routeSourceHeader   routeSource = "header"
	routeSourceHost     routeSource = "host"
	routeSourcePath     routeSource = "path"
	routeSourceSchedule routeSource = "schedule"
	routeSourceGateway  routeSource = "gateway"
)

type ServerOptions struct {
	APIKey                   string
	RequestTimeout           time.Duration
	MaxResponseSize          int64
	DebugMode                bool
	SandboxProxyDomains      []string
	QueryOnlySchedulerClient schedulerv1.SchedulerClient
	// MaxIdleConnsPerHost bounds pooled idle connections per node. Zero uses
	// defaultMaxIdleConnsPerHost.
	MaxIdleConnsPerHost int
	// BindingCacheTTL bounds how long a sandbox-to-node lookup is reused. Zero
	// uses defaultBindingCacheTTL.
	BindingCacheTTL time.Duration
	// MaxInFlightCreates bounds concurrent create placements. Zero uses
	// defaultMaxInFlightCreates.
	MaxInFlightCreates int
	// MaxScheduleRetries bounds how many further nodes a create is offered to
	// after one refuses it. Zero uses defaultMaxScheduleRetries; a negative
	// value gives every create a single attempt.
	MaxScheduleRetries int
}

// defaultMaxIdleConnsPerHost is sized for a gateway fronting a handful of nodes
// with many concurrent sandboxes each, rather than Go's default of 2, which is
// tuned for a client talking to many distinct hosts.
const defaultMaxIdleConnsPerHost = 256

func newUpstreamTransport(maxIdleConnsPerHost int) *http.Transport {
	if maxIdleConnsPerHost <= 0 {
		maxIdleConnsPerHost = defaultMaxIdleConnsPerHost
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = maxIdleConnsPerHost
	// The pool is per-node, so the global cap has to allow for several nodes'
	// worth of idle connections or it silently undoes the per-host budget.
	transport.MaxIdleConns = maxIdleConnsPerHost * 8
	transport.IdleConnTimeout = 90 * time.Second
	return transport
}

type Server struct {
	logger             *zap.Logger
	scheduler          schedulerv1.SchedulerClient
	queryOnlyScheduler schedulerv1.SchedulerClient
	httpClient         *http.Client
	// upstreamTransport is shared by every proxied request so connections to a
	// node are pooled rather than re-established per request.
	upstreamTransport *http.Transport
	// bindingCache is the same object as queryOnlyScheduler, kept typed so the
	// proxy can invalidate an entry the upstream has just contradicted.
	bindingCache *CachingSchedulerClient
	// createLimiter bounds concurrent create placements so a burst cannot
	// multiply into scheduler load through the reschedule loop.
	createLimiter *createLimiter
	// maxScheduleRetries is how many times one create may be re-placed after a
	// node refuses it. See defaultMaxScheduleRetries for why it is bounded.
	maxScheduleRetries int
	apiKey             []byte
	requestTimeout     time.Duration
	maxRespSize        int64
	// debugMode, when true, enables debug-only behaviors such as exposing
	// the backend node id on proxied responses via the x-agentenv-node-id
	// header. Off by default; toggled via GatewayConfig.DebugMode.
	debugMode           bool
	sandboxProxyDomains []string
}

func NewServer(logger *zap.Logger, schedulerClient schedulerv1.SchedulerClient, options ServerOptions) (*Server, error) {
	if options.APIKey == "" {
		return nil, errors.New("API key is required")
	}
	sandboxProxyDomains, err := normalizeProxyDomains(options.SandboxProxyDomains)
	if err != nil {
		return nil, err
	}

	queryOnlyScheduler := options.QueryOnlySchedulerClient
	if queryOnlyScheduler == nil {
		queryOnlyScheduler = schedulerClient
	}
	// Wrap the data-plane lookup path only. Scheduling and assignment writes
	// keep talking to the scheduler directly; caching those would cache
	// decisions rather than facts.
	bindingCache := NewCachingSchedulerClient(queryOnlyScheduler, options.BindingCacheTTL)
	queryOnlyScheduler = bindingCache

	upstreamTransport := newUpstreamTransport(options.MaxIdleConnsPerHost)

	return &Server{
		logger:             logger,
		scheduler:          schedulerClient,
		queryOnlyScheduler: queryOnlyScheduler,
		httpClient: &http.Client{
			Transport: upstreamTransport,
		},
		upstreamTransport:   upstreamTransport,
		bindingCache:        bindingCache,
		createLimiter:       newCreateLimiter(options.MaxInFlightCreates),
		maxScheduleRetries:  scheduleRetryBound(options.MaxScheduleRetries),
		requestTimeout:      options.RequestTimeout,
		maxRespSize:         options.MaxResponseSize,
		apiKey:              []byte(options.APIKey),
		debugMode:           options.DebugMode,
		sandboxProxyDomains: sandboxProxyDomains,
	}, nil
}

func (s *Server) SandboxProxyDomains() []string {
	return s.sandboxProxyDomains
}

func (s *Server) Handler() http.Handler {
	// We avoid http.ServeMux because it normalizes request paths (e.g.
	// decoding %2F → / and issuing 301 redirects), which breaks proxy
	// forwarding of percent-encoded path segments such as /files/%2F.
	core := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isExplicitProxyPath(r.URL.Path) && !hasCompleteProxyRouteHeaders(r.Header) {
			setGatewayRouteSource(w, routeSourceHeader)
			if _, hasSandbox := sandboxIDFromHeaders(r.Header); !hasSandbox {
				http.Error(w, "sandbox id header required", http.StatusBadRequest)
				return
			}
			http.Error(w, "target port header required", http.StatusBadRequest)
			return
		}
		if r.URL.Path == "/health" || r.URL.Path == "/metrics" {
			hostRoute, hostRouteErr := parseHostRoute(r.Host, s.sandboxProxyDomains)
			if hostRoute != nil || hostRouteErr != nil {
				s.handleProxy(w, r)
				return
			}
			if hasProxyRoutingHeaders(r.Header) {
				if _, hasSandbox := sandboxIDFromHeaders(r.Header); !hasSandbox {
					setGatewayRouteSource(w, routeSourceHeader)
					http.Error(w, "sandbox id header required", http.StatusBadRequest)
					return
				}
				s.handleProxy(w, r)
				return
			}
			if r.URL.Path == "/health" {
				// Keep load balancer health checks local when they are not sandbox-routed.
				w.WriteHeader(http.StatusNoContent)
			} else {
				// Gateway Prometheus metrics use the separate metrics listener. Keep
				// this path unavailable on the public HTTP listener unless it is
				// explicitly routed to a sandbox.
				http.NotFound(w, r)
			}
			return
		}
		s.handleProxy(w, r)
	})
	return s.instrumentGatewayHTTP(s.authenticate(core))
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, value any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(value); err != nil {
		s.logger.Warn("encode json response failed",
			zap.Error(err),
			zap.Int("status", status),
		)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := buf.WriteTo(w); err != nil {
		s.logger.Warn("write json response failed",
			zap.Error(err),
			zap.Int("status", status),
		)
	}
}

func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	websocket := isWebSocketRequest(r)
	streaming := isStreamingRequest(r)
	longLived := streaming || websocket
	routingCtx, cancelRouting := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancelRouting()

	hostRoute, hostRouteErr := parseHostRoute(r.Host, s.sandboxProxyDomains)
	if hostRouteErr != nil {
		setGatewayRouteSource(w, routeSourceHost)
		s.logger.Debug("host routing rejected",
			zap.String("host", r.Host),
			zap.Error(hostRouteErr),
			zap.Int("status", http.StatusBadRequest),
		)
		http.Error(w, hostRouteErr.Error(), http.StatusBadRequest)
		return
	}

	if hostRoute == nil && !hasProxyRoutingHeaders(r.Header) {
		if isClusterListRequest(r) {
			setGatewayRouteSource(w, routeSourceGateway)
			s.handleClusterList(w, r, routingCtx)
			return
		} else if isNodeListRequest(r) {
			setGatewayRouteSource(w, routeSourceGateway)
			s.handleNodeList(w, r, routingCtx)
			return
		} else if nodeID, ok := isNodeDetailRequest(r); ok {
			setGatewayRouteSource(w, routeSourcePath)
			s.handleNodeDetail(w, r, routingCtx, nodeID, longLived)
			return
		}
	}

	sandboxID, hasSandbox := "", false
	routeSource := routeSourceHeader
	if hostRoute != nil {
		s.logHostRoutingHeaderConflicts(r, hostRoute)
		sandboxID = hostRoute.sandboxID
		hasSandbox = true
		routeSource = routeSourceHost
	} else if isSandboxControlPlaneRequest(r) {
		sandboxID, hasSandbox = sandboxIDFromPath(r.URL.Path)
		routeSource = routeSourcePath
	} else {
		sandboxID, hasSandbox = sandboxIDFromHeaders(r.Header)
	}
	if !hasSandbox {
		routeSource = routeSourceSchedule
	}
	setGatewayRouteSource(w, routeSource)
	var node *schedulerv1.Node

	if hasSandbox {
		rpcStart := time.Now()
		resp, err := s.queryOnlyScheduler.LookupNode(routingCtx, &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
		recordGatewaySchedulerRPC("LookupNode", rpcStart, err)
		if err != nil {
			s.writeSchedulerError(w, err)
			return
		}
		node = resp.GetNode()
	} else if isScheduledCreateRequest(r) {
		hint, err := buildScheduleHint(r)
		if err != nil {
			// this only happens it cannot read request body, so the request cannot continue
			s.logger.Warn("Fatal error when building schedule hint",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Error(err),
			)
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
		// A create can be offered to more than one node: the node's own
		// admission decision is authoritative over the scheduler's stale
		// capacity view, so a rejection must steer the placement rather than
		// surface as a failure. Everything from here to the upstream call is
		// therefore run in a loop, bounded by maxScheduleRetries.
		s.proxyScheduledCreate(w, r, routingCtx, hint, routeSource, hostRoute, longLived)
		return
	} else {
		// Anything else without a sandbox is management-plane traffic —
		// templates, snapshots, builds — that merely needs some node. It is
		// not a create, so it is neither shed nor retried: a 503 to a DELETE
		// is the node's answer, and re-running the DELETE elsewhere is not.
		s.proxyManagementRequest(w, r, routingCtx, routeSource, longLived)
		return
	}

	s.logger.Debug("gateway routed request",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("route_source", string(routeSource)),
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", node.GetNodeId()),
		zap.String("upstream_endpoint", node.GetEndpoint()),
	)

	decodedPath := upstreamTargetPath(routeSource, r.URL.Path)
	escapedPath := upstreamTargetEscapedPath(routeSource, requestEscapedPath(r))
	upstreamURL, err := joinUpstream(node.GetEndpoint(), decodedPath, escapedPath, r.URL.RawQuery)
	if err != nil {
		http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
		return
	}

	upstreamCtx, cancelUpstream := requestContextForProxy(r, routingCtx, longLived)
	defer cancelUpstream()

	options := proxyRequestOptions{
		recordAssignment: shouldRecordAssignment(r, routeSource, hasSandbox),
		hostRoute:        hostRoute,
		sandboxID:        sandboxID,
		flushImmediately: longLived,
	}

	// A sandbox that moved between the lookup and the request would otherwise
	// fail with the old node's 404, which reads to the client as "your sandbox
	// is gone" rather than "it is somewhere else now".
	if s.proxyWithCutover(w, r, routingCtx, upstreamCtx, node, options, sandboxID, routeSource, longLived) {
		return
	}

	s.proxyRequest(
		w,
		r.Clone(upstreamCtx),
		r.Context(),
		upstreamURL,
		node,
		options,
	)
}

// proxyScheduledCreate places a create, retrying on another node when the
// chosen one refuses it.
//
// The response is buffered so a retryable rejection can be discarded before it
// reaches the client. Creates are small non-streaming responses; streaming and
// long-lived requests never reach this path.
func (s *Server) proxyScheduledCreate(
	w http.ResponseWriter,
	r *http.Request,
	routingCtx context.Context,
	hint *schedulerv1.ScheduleRequestHint,
	routeSource routeSource,
	hostRoute *hostRoute,
	longLived bool,
) {
	// Shed before placing anything. A create is the only request that can fan
	// out across several nodes, so admitting more than the gateway will carry
	// turns one burst into a multiple of it against the scheduler.
	release, ok := s.createLimiter.acquire()
	if !ok {
		recordGatewayRefusal(refusalGatewayShed)
		s.logger.Warn("gateway shedding create; too many placements in flight",
			zap.Int64("in_flight", s.createLimiter.currentInFlight()),
		)
		writeRefusal(w, refusalGatewayShed, 1)
		return
	}
	defer release()

	// captureRequestBody leaves a replayable body only when it fitted the hint
	// budget. A larger body is a one-shot stream, so it gets a single attempt
	// rather than a silently truncated retry.
	replayableBody, bodyIsReplayable := replayableRequestBody(r)

	var excluded []string
	for attempt := 0; ; attempt++ {
		rpcStart := time.Now()
		resp, err := s.scheduler.Schedule(routingCtx, &schedulerv1.ScheduleRequest{
			Hint:           hint,
			ExcludeNodeIds: excluded,
		})
		recordGatewaySchedulerRPC("Schedule", rpcStart, err)
		if err != nil {
			// Exhausting the candidate set after some nodes refused is a
			// different answer from "the fleet has no capacity at all", but
			// both mean this create cannot be placed right now.
			if status.Code(err) == codes.Unavailable {
				recordGatewayRefusal(refusalFleetExhausted)
				writeRefusal(w, refusalFleetExhausted, 2)
				return
			}
			s.writeSchedulerError(w, err)
			return
		}
		node := resp.GetNode()

		decodedPath := upstreamTargetPath(routeSource, r.URL.Path)
		escapedPath := upstreamTargetEscapedPath(routeSource, requestEscapedPath(r))
		upstreamURL, joinErr := joinUpstream(node.GetEndpoint(), decodedPath, escapedPath, r.URL.RawQuery)
		if joinErr != nil {
			http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
			return
		}

		lastAttempt := attempt >= s.maxScheduleRetries || !bodyIsReplayable
		upstreamCtx, cancelUpstream := requestContextForProxy(r, routingCtx, longLived)
		proxyReq := r.Clone(upstreamCtx)
		if bodyIsReplayable {
			setReplayableBody(proxyReq, replayableBody)
		}

		options := proxyRequestOptions{
			recordAssignment: shouldRecordAssignment(r, routeSource, false),
			hostRoute:        hostRoute,
			flushImmediately: longLived,
		}

		if lastAttempt {
			s.proxyRequest(w, proxyReq, r.Context(), upstreamURL, node, options)
			cancelUpstream()
			return
		}

		// Bounded for the same reason the cutover buffer is: a create response
		// is small, but the bound is what makes that an assumption the gateway
		// can survive being wrong about.
		buffered := newBoundedBufferedResponse(s.maxRespSize, w)
		s.proxyRequest(buffered, proxyReq, r.Context(), upstreamURL, node, options)
		cancelUpstream()

		if buffered.spilled {
			return
		}
		if !retryableRejection(buffered.status, buffered.header) {
			buffered.replay(w)
			return
		}

		recordGatewayScheduleRetry()
		s.logger.Debug("node refused create; rescheduling",
			zap.String("node_id", node.GetNodeId()),
			zap.Int("attempt", attempt+1),
		)
		excluded = append(excluded, node.GetNodeId())
	}
}

// proxyManagementRequest forwards a sandbox-less, non-create request to
// whichever node the scheduler picks, once.
//
// Nothing here is retried. Only a create is safe to offer to a second node
// after the first said no; a management request that a node refused has been
// answered, and that answer belongs to the client.
func (s *Server) proxyManagementRequest(
	w http.ResponseWriter,
	r *http.Request,
	routingCtx context.Context,
	routeSource routeSource,
	longLived bool,
) {
	rpcStart := time.Now()
	resp, err := s.scheduler.Schedule(routingCtx, &schedulerv1.ScheduleRequest{})
	recordGatewaySchedulerRPC("Schedule", rpcStart, err)
	if err != nil {
		s.writeSchedulerError(w, err)
		return
	}
	node := resp.GetNode()

	upstreamURL, err := joinUpstream(
		node.GetEndpoint(),
		upstreamTargetPath(routeSource, r.URL.Path),
		upstreamTargetEscapedPath(routeSource, requestEscapedPath(r)),
		r.URL.RawQuery,
	)
	if err != nil {
		http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
		return
	}

	upstreamCtx, cancelUpstream := requestContextForProxy(r, routingCtx, longLived)
	defer cancelUpstream()
	s.proxyRequest(w, r.Clone(upstreamCtx), r.Context(), upstreamURL, node, proxyRequestOptions{
		flushImmediately: longLived,
	})
}

func (s *Server) writeSchedulerError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		http.Error(w, "scheduler unavailable", http.StatusBadGateway)
		return
	}
	switch st.Code() {
	case codes.InvalidArgument:
		http.Error(w, st.Message(), http.StatusBadRequest)
	case codes.NotFound:
		http.Error(w, st.Message(), http.StatusNotFound)
	case codes.Unavailable:
		http.Error(w, st.Message(), http.StatusServiceUnavailable)
	default:
		http.Error(w, "scheduler error", http.StatusBadGateway)
	}
}

type proxyRequestOptions struct {
	recordAssignment bool
	hostRoute        *hostRoute
	// sandboxID is the sandbox the request was routed by, however it was
	// named. A host-routed request carries it only in the Host label and a
	// control-plane request only in the path, so the response side cannot
	// recover it from headers.
	sandboxID        string
	flushImmediately bool
	// onDisown fires when the node says it does not have this sandbox, so a
	// caller that can retry elsewhere knows to.
	onDisown func()
}

func (s *Server) proxyRequest(
	w http.ResponseWriter,
	proxyReq *http.Request,
	originalCtx context.Context,
	target string,
	node *schedulerv1.Node,
	options proxyRequestOptions,
) {
	upstreamURL, err := url.Parse(target)
	if err != nil {
		http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
		return
	}

	proxy := &httputil.ReverseProxy{
		// Share one transport across requests. The default builds a fresh
		// ReverseProxy per request with no Transport set, which falls back to
		// http.DefaultTransport and its MaxIdleConnsPerHost of 2 — so beyond
		// two concurrent requests to a node, every request paid a fresh TCP
		// handshake, and the gateway burned ephemeral ports doing it.
		Transport: s.upstreamTransport,
		Rewrite: func(req *httputil.ProxyRequest) {
			req.Out.URL.Scheme = upstreamURL.Scheme
			req.Out.URL.Host = upstreamURL.Host
			req.Out.URL.Path = upstreamURL.Path
			req.Out.URL.RawPath = upstreamURL.RawPath
			req.Out.URL.RawQuery = upstreamURL.RawQuery
			req.Out.Host = req.In.Host
			injectForwardedHeaders(req.Out.Header, req.In)
			if options.hostRoute != nil {
				req.Out.Header.Set(headerSandboxID, options.hostRoute.sandboxID)
				req.Out.Header.Set(headerTargetPort, strconv.Itoa(options.hostRoute.targetPort))
			}
		},
		FlushInterval: flushInterval(options.flushImmediately),
		ModifyResponse: func(resp *http.Response) error {
			// In debug mode, expose the upstream node id on the response so
			// operators can tell which backend node served a given request.
			// This is purely for debugging/observability and is not consumed
			// by the client.
			if s.debugMode {
				if nodeID := node.GetNodeId(); nodeID != "" {
					resp.Header.Set(headerNodeID, nodeID)
				}
			}
			// A node saying it does not have this sandbox means the cached
			// binding is wrong now, not in a second. This used to fire on any
			// 404 or 502, which meant every 404 the guest's own application
			// returned cost a scheduler round trip; the node now says which it
			// is, and only a real disown invalidates.
			if isSandboxDisowned(resp) {
				if options.sandboxID != "" {
					s.bindingCache.Invalidate(options.sandboxID)
				} else if sandboxID, ok := sandboxIDFromHeaders(proxyReq.Header); ok {
					s.bindingCache.Invalidate(sandboxID)
				}
				if options.onDisown != nil {
					options.onDisown()
				}
			}
			if !options.recordAssignment || resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return nil
			}
			// Recording never fails the response: the node has already created
			// the sandboxes, so a 502 here would hide identifiers the client
			// can never recover.
			s.recordAssignmentFromResponse(originalCtx, resp, node)
			return nil
		},
		ErrorHandler: func(rw http.ResponseWriter, _ *http.Request, err error) {
			if errors.Is(err, context.Canceled) {
				logLevel := zap.WarnLevel
				if isStreamInputProxyRequest(proxyReq) {
					logLevel = zap.DebugLevel
				}
				s.logger.Log(logLevel, "proxy request closed by client",
					zap.Error(err),
					zap.String("node", node.GetNodeId()),
					zap.String("path", proxyReq.URL.Path),
					zap.String("target", upstreamURL.String()),
				)
				return
			}

			if errors.Is(err, context.DeadlineExceeded) || errors.Is(proxyReq.Context().Err(), context.DeadlineExceeded) {
				s.logger.Warn("proxy request timed out",
					zap.Error(err),
					zap.String("node", node.GetNodeId()),
					zap.String("path", proxyReq.URL.Path),
					zap.String("target", upstreamURL.String()),
				)
				http.Error(rw, "upstream timeout", http.StatusGatewayTimeout)
				return
			}

			s.logger.Warn("proxy request failed",
				zap.Error(err),
				zap.String("node", node.GetNodeId()),
				zap.String("path", proxyReq.URL.Path),
				zap.String("target", upstreamURL.String()),
			)
			http.Error(rw, "upstream unavailable", http.StatusBadGateway)
		},
	}

	proxyStart := time.Now()
	route := gatewayRouteLabel(proxyReq.URL.Path)
	proxy.ServeHTTP(w, proxyReq)
	recordGatewayUpstreamProxy(route, proxyStart, w, proxyReq.Context())
}

func isStreamInputProxyRequest(r *http.Request) bool {
	return r.Method == http.MethodPost && r.URL.Path == "/process.Process/StreamInput"
}

// recordAssignmentFromResponse records the bindings for the sandboxes an
// upstream response just created.
//
// It never fails the response. The node has already created the sandboxes by
// the time this runs, and returning an error here turns a successful create
// into a 502 the client cannot act on — the sandboxes exist either way, and
// the client is not told their identifiers. Every failure path therefore logs
// and returns nil; a missed binding is repaired by the owning node's next
// heartbeat reconcile.
func (s *Server) recordAssignmentFromResponse(ctx context.Context, resp *http.Response, node *schedulerv1.Node) {
	recordCtx, cancelRecord := context.WithTimeout(ctx, recordAssignmentTimeout(s.requestTimeout))
	defer cancelRecord()

	if sandboxID, ok := sandboxIDFromHeaders(resp.Header); ok {
		s.recordAssignment(recordCtx, sandboxID, node, "response_header")
		return
	}

	body, truncated, err := readBodyWithLimit(resp.Body, s.maxRespSize)
	if err != nil {
		s.logger.Warn("failed to read upstream response while recording assignment",
			zap.Error(err),
			zap.String("node_id", node.GetNodeId()),
		)
		// Reattach what was read so the client still receives the prefix.
		//
		// prefixedBody, not io.NopCloser: wrapping in a NopCloser drops the
		// upstream body's Close, so the connection is never released and its
		// transport goroutine never exits. On a proxy that is a leak per
		// oversized or unreadable response.
		resp.Body = &prefixedBody{
			Reader: io.MultiReader(bytes.NewReader(body), resp.Body),
			closer: resp.Body,
		}
		return
	}
	if truncated {
		s.logger.Warn("upstream response exceeded configured forwarding limit; skipping assignment recording",
			zap.Int64("max_response_size_bytes", s.maxRespSize),
			zap.Int64("upstream_content_length", resp.ContentLength),
			zap.String("content_type", resp.Header.Get("Content-Type")),
		)
		resp.Body = &prefixedBody{
			Reader: io.MultiReader(bytes.NewReader(body), resp.Body),
			closer: resp.Body,
		}
		return
	}
	_ = resp.Body.Close()

	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	if resp.Header == nil {
		resp.Header = make(http.Header)
	}
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))

	s.recordAssignments(recordCtx, extractSandboxIDsFromResponse(body), node, "response_body")
}

// recordAssignments records a set of bindings in one RPC. Fork returns up to
// 100 children in a single response; recording them one RPC at a time
// serialized that many round trips inside one deadline, so the tail was
// silently dropped once the deadline passed.
func (s *Server) recordAssignments(ctx context.Context, sandboxIDs []string, node *schedulerv1.Node, source string) {
	switch len(sandboxIDs) {
	case 0:
		return
	case 1:
		s.recordAssignment(ctx, sandboxIDs[0], node, source)
		return
	}

	assignments := make([]*schedulerv1.RecordAssignmentRequest, 0, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		assignments = append(assignments, &schedulerv1.RecordAssignmentRequest{SandboxId: sandboxID, Node: node})
	}

	rpcStart := time.Now()
	resp, err := s.scheduler.RecordAssignments(ctx, &schedulerv1.RecordAssignmentsRequest{Assignments: assignments})
	recordGatewaySchedulerRPC("RecordAssignments", rpcStart, err)
	if err != nil {
		s.logger.Warn("record assignments failed",
			zap.Error(err),
			zap.Int("sandbox_count", len(sandboxIDs)),
			zap.String("node_id", node.GetNodeId()),
		)
		return
	}

	for _, result := range resp.GetResults() {
		if result.GetError() == "" {
			continue
		}
		s.logger.Warn("record assignment failed",
			zap.String("sandbox_id", result.GetSandboxId()),
			zap.String("node_id", node.GetNodeId()),
			zap.String("error", result.GetError()),
		)
	}

	s.logger.Debug("gateway recorded sandbox assignments",
		zap.Int("sandbox_count", len(sandboxIDs)),
		zap.String("node_id", node.GetNodeId()),
		zap.String("source", source),
	)
}

func (s *Server) recordAssignment(ctx context.Context, sandboxID string, node *schedulerv1.Node, source string) {
	rpcStart := time.Now()
	_, err := s.scheduler.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{SandboxId: sandboxID, Node: node})
	recordGatewaySchedulerRPC("RecordAssignment", rpcStart, err)
	if err != nil {
		s.logger.Warn("record assignment failed", zap.Error(err), zap.String("sandbox_id", sandboxID), zap.String("node_id", node.GetNodeId()))
		return
	}

	s.logger.Debug("gateway recorded sandbox assignment",
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", node.GetNodeId()),
		zap.String("source", source),
	)
}

// readBodyWithLimit reads up to limit bytes from src.
//
// On truncation it returns the bytes it consumed along with truncated=true.
// Callers that forward the response must reattach that prefix to the unread
// remainder, or the client receives a body missing its first limit+1 bytes.
func readBodyWithLimit(src io.Reader, limit int64) ([]byte, bool, error) {
	if limit <= 0 {
		body, err := io.ReadAll(src)
		return body, false, err
	}
	body, err := io.ReadAll(io.LimitReader(src, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(body)) > limit {
		return body, true, nil
	}
	return body, false, nil
}

func recordAssignmentTimeout(requestTimeout time.Duration) time.Duration {
	if requestTimeout <= 0 {
		return maxRecordAssignmentTimeout
	}
	if requestTimeout < maxRecordAssignmentTimeout {
		return requestTimeout
	}
	return maxRecordAssignmentTimeout
}

func flushInterval(flushImmediately bool) time.Duration {
	if flushImmediately {
		return -1
	}
	return 0
}

// isScheduledCreateRequest reports whether a sandbox-less request creates a
// sandbox. These are the only requests the placement loop is for: the only
// ones a node may refuse for capacity, and the only ones safe to offer to a
// second node after the first said no.
func isScheduledCreateRequest(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	switch strings.TrimRight(r.URL.Path, "/") {
	case "/sandboxes", "/sandboxes-cold":
		return true
	default:
		return false
	}
}

func shouldRecordAssignment(r *http.Request, routeSource routeSource, hasSandbox bool) bool {
	if !hasSandbox {
		return isScheduledCreateRequest(r)
	}
	if r.Method != http.MethodPost || routeSource != routeSourcePath {
		return false
	}

	// Fork is routed by the source sandbox but creates child sandbox assignments.
	path := strings.TrimRight(r.URL.Path, "/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	return len(parts) == 3 && parts[0] == "sandboxes" && strings.TrimSpace(parts[1]) != "" && parts[2] == "fork"
}

func sandboxIDFromHeaders(h http.Header) (string, bool) {
	for _, name := range []string{headerSandboxID, headerE2BSandboxID} {
		v := strings.TrimSpace(h.Get(name))
		if v != "" {
			return v, true
		}
	}
	return "", false
}

func hasProxyRoutingHeaders(h http.Header) bool {
	for _, name := range []string{
		headerSandboxID,
		headerE2BSandboxID,
		headerTargetPort,
		headerE2BTargetPort,
	} {
		if strings.TrimSpace(h.Get(name)) != "" {
			return true
		}
	}
	return false
}

func hasCompleteProxyRouteHeaders(h http.Header) bool {
	_, hasSandbox := sandboxIDFromHeaders(h)
	_, hasTargetPort := targetPortFromHeaders(h)
	return hasSandbox && hasTargetPort
}

func targetPortFromHeaders(h http.Header) (string, bool) {
	for _, name := range []string{headerTargetPort, headerE2BTargetPort} {
		v := strings.TrimSpace(h.Get(name))
		if v != "" {
			return v, true
		}
	}
	return "", false
}

func sandboxIDFromPath(path string) (string, bool) {
	const marker = "/sandboxes/"
	rest, found := strings.CutPrefix(path, marker)
	if !found {
		_, rest, found = strings.Cut(path, marker)
	}
	if !found {
		return "", false
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return "", false
	}
	if id, _, hasSlash := strings.Cut(rest, "/"); hasSlash {
		rest = id
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return "", false
	}
	return rest, true
}

func isSandboxControlPlaneRequest(r *http.Request) bool {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "sandboxes" || strings.TrimSpace(parts[1]) == "" {
		return false
	}

	if len(parts) == 2 {
		return r.Method == http.MethodGet || r.Method == http.MethodDelete
	}
	if len(parts) != 3 {
		return false
	}

	switch parts[2] {
	case "pause", "resume", "fork", "connect", "timeout", "refreshes", "snapshots":
		return r.Method == http.MethodPost
	case "network":
		return r.Method == http.MethodPut
	case "custom-extension-params":
		return r.Method == http.MethodGet || r.Method == http.MethodPatch
	default:
		return false
	}
}

func (s *Server) logHostRoutingHeaderConflicts(r *http.Request, route *hostRoute) {
	headerSandboxIDValue, hasHeaderSandboxID := sandboxIDFromHeaders(r.Header)
	headerTargetPortValue, hasHeaderTargetPort := targetPortFromHeaders(r.Header)

	hostTargetPortValue := strconv.Itoa(route.targetPort)
	sandboxIDConflict := hasHeaderSandboxID && headerSandboxIDValue != route.sandboxID
	targetPortConflict := hasHeaderTargetPort && headerTargetPortValue != hostTargetPortValue
	if !sandboxIDConflict && !targetPortConflict {
		return
	}

	s.logger.Debug("host routing overrides conflicting routing headers",
		zap.String("host", r.Host),
		zap.String("host_sandbox_id", route.sandboxID),
		zap.String("host_target_port", hostTargetPortValue),
		zap.String("header_sandbox_id", headerSandboxIDValue),
		zap.String("header_target_port", headerTargetPortValue),
		zap.Bool("sandbox_id_conflict", sandboxIDConflict),
		zap.Bool("target_port_conflict", targetPortConflict),
	)
}

func isDataPlaneRouteSource(routeSource routeSource) bool {
	return routeSource == routeSourceHeader || routeSource == routeSourceHost
}

// upstreamTargetPath returns the path to use when forwarding to the upstream
// node. Requests routed via sandbox proxy host or routing headers are forwarded
// to the /proxy sub-tree on the upstream, while control-plane and scheduled
// requests are forwarded as-is.
func upstreamTargetPath(routeSource routeSource, originalPath string) string {
	if isDataPlaneRouteSource(routeSource) {
		return "/proxy" + originalPath
	}
	return originalPath
}

func upstreamTargetEscapedPath(routeSource routeSource, originalEscapedPath string) string {
	if isDataPlaneRouteSource(routeSource) {
		return "/proxy" + originalEscapedPath
	}
	return originalEscapedPath
}

func joinUpstream(endpoint string, path string, escapedPath string, rawQuery string) (string, error) {
	base, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	if base.Scheme == "" || base.Host == "" {
		return "", errors.New("endpoint must include scheme and host")
	}
	baseEscapedPath := base.EscapedPath()
	base.Path = joinURLPath(base.Path, path)
	if escapedPath != "" {
		base.RawPath = joinURLPath(baseEscapedPath, escapedPath)
	}
	base.RawQuery = rawQuery
	return base.String(), nil
}

func requestEscapedPath(r *http.Request) string {
	if raw := r.URL.RawPath; raw != "" {
		return raw
	}
	if uri := strings.TrimSpace(r.RequestURI); uri != "" {
		if parsed, err := url.ParseRequestURI(uri); err == nil {
			if escaped := parsed.EscapedPath(); escaped != "" {
				return escaped
			}
		}
	}
	if escaped := r.URL.EscapedPath(); escaped != "" {
		return escaped
	}
	return "/"
}

func joinURLPath(basePath, path string) string {
	switch {
	case strings.HasSuffix(basePath, "/") && strings.HasPrefix(path, "/"):
		return basePath + strings.TrimPrefix(path, "/")
	case !strings.HasSuffix(basePath, "/") && !strings.HasPrefix(path, "/"):
		if basePath == "" {
			return "/" + path
		}
		return basePath + "/" + path
	default:
		if basePath == "" {
			return "/" + strings.TrimPrefix(path, "/")
		}
		return basePath + path
	}
}

func injectForwardedHeaders(h http.Header, r *http.Request) {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	setXForwardedFor(h, r.RemoteAddr)
	h.Set("X-Forwarded-Host", r.Host)
	h.Set("X-Forwarded-Proto", scheme)
	h.Set("X-Forwarded-Method", r.Method)
	h.Set("X-Forwarded-URI", r.URL.RequestURI())
}

func setXForwardedFor(h http.Header, remoteAddr string) {
	host := strings.TrimSpace(remoteAddr)
	if parsedHost, _, err := net.SplitHostPort(remoteAddr); err == nil {
		host = parsedHost
	}
	if host == "" {
		h.Del("X-Forwarded-For")
		return
	}
	h.Set("X-Forwarded-For", host)
}

func requestContextForProxy(r *http.Request, routingCtx context.Context, streaming bool) (context.Context, context.CancelFunc) {
	if streaming {
		return r.Context(), func() {}
	}
	return routingCtx, func() {}
}

func isStreamingRequest(r *http.Request) bool {
	contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if strings.HasPrefix(contentType, "application/grpc") {
		return true
	}
	if strings.HasPrefix(contentType, "application/connect+") {
		return true
	}
	if strings.TrimSpace(r.Header.Get("Connect-Protocol-Version")) != "" {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("Accept")), "text/event-stream") {
		return true
	}
	if headerContainsToken(r.Header, "Te", "trailers") {
		return true
	}
	return false
}

func isWebSocketRequest(r *http.Request) bool {
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket") &&
		headerContainsToken(r.Header, "Connection", "upgrade")
}

func headerContainsToken(h http.Header, name string, want string) bool {
	for _, v := range h.Values(name) {
		for _, token := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(token), want) {
				return true
			}
		}
	}
	return false
}

func extractSandboxIDFromResponse(body []byte) (string, bool) {
	ids := extractSandboxIDsFromResponse(body)
	if len(ids) == 0 {
		return "", false
	}
	return ids[0], true
}

// sandboxIDKeys are the property names a sandbox identifier can appear under
// in a node response body.
var sandboxIDKeys = [...]string{"sandboxID", "sandboxId", "sandbox_id"}

// maxSandboxIDSearchDepth bounds the walk below. The deepest shape any endpoint
// produces is a top-level array of objects carrying a nested `sandbox` object,
// which is depth 3.
const maxSandboxIDSearchDepth = 3

// extractSandboxIDsFromResponse collects the sandbox identifiers a create-like
// response describes.
//
// The response shapes differ per endpoint: POST /sandboxes and
// POST /sandboxes-cold return a single object (and also set the
// x-agentenv-sandbox-id header, so they rarely reach here), while
// POST /sandboxes/{id}/fork returns a bare JSON array of per-child results,
// each carrying either a nested `sandbox` object or an `error`. Decoding into
// a concrete shape therefore silently drops whichever shape it does not match,
// so this walks the decoded document instead, bounded by depth.
func extractSandboxIDsFromResponse(body []byte) []string {
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil
	}

	var ids []string
	var walk func(node any, depth int)
	walk = func(node any, depth int) {
		if depth > maxSandboxIDSearchDepth {
			return
		}
		switch value := node.(type) {
		case map[string]any:
			for _, key := range sandboxIDKeys {
				if id, ok := value[key].(string); ok && strings.TrimSpace(id) != "" {
					ids = append(ids, id)
				}
			}
			// Descend only through the container keys that carry sandboxes, so
			// an unrelated string field named like an ID elsewhere in the
			// document cannot be mistaken for one.
			for _, key := range [...]string{"sandbox", "sandboxes", "data", "items"} {
				if child, ok := value[key]; ok {
					walk(child, depth+1)
				}
			}
		case []any:
			for _, item := range value {
				walk(item, depth+1)
			}
		}
	}
	walk(payload, 0)

	if len(ids) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ids))
	unique := ids[:0]
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}

func singleHeaderMatches(headers http.Header, name string, expected []byte) bool {
	values := headers.Values(name)
	if len(values) != 1 || len(values[0]) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(values[0]), expected) == 1
}

func (s *Server) isSandboxDataPlaneRequest(r *http.Request) bool {
	if isExplicitProxyPath(r.URL.Path) {
		// The explicit proxy prefix cannot dispatch to a control-plane handler.
		// Let the core handler return a stable 400 for incomplete routing data.
		return true
	}

	hostRoute, err := parseHostRoute(r.Host, s.sandboxProxyDomains)
	if hostRoute != nil {
		return true
	}
	if err != nil {
		return false
	}

	return !isSandboxControlPlaneRequest(r) && hasCompleteProxyRouteHeaders(r.Header)
}

func isExplicitProxyPath(path string) bool {
	return path == "/proxy" || strings.HasPrefix(path, "/proxy/")
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dataPlane := s.isSandboxDataPlaneRequest(r)
		if dataPlane || r.URL.Path == "/health" || r.URL.Path == "/metrics" {
			// Sandbox-scoped ingress and envd authorization depend on runtime
			// metadata and are enforced by the owning runtime node.
			next.ServeHTTP(w, r)
			return
		}

		if !singleHeaderMatches(r.Header, headerAPIKey, s.apiKey) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
