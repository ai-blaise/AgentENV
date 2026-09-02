package scheduler

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// The wire contract for scheduler gRPC authentication. Clients — the gateway
// and the node's heartbeat reporter — send the shared token as
//
//	authorization: Bearer <token>
//
// in the request metadata. The scheme is matched case-insensitively, as HTTP
// defines it; the token is matched exactly.
const (
	AuthMetadataKey  = "authorization"
	AuthBearerScheme = "Bearer"
)

// healthServicePrefix names the one service that stays open when a token is
// configured. Kubernetes probes the listener with grpc_health_probe, which
// has no token; closing it would make every replica unready the moment auth
// was switched on. The health service answers nothing about placement state.
const healthServicePrefix = "/grpc.health.v1.Health/"

// maxAuthTokenLen bounds a token read from a file, so a wrong path — a
// certificate bundle, a log — fails as "too long" rather than being loaded
// and compared on every RPC.
const maxAuthTokenLen = 4096

// Reasons an RPC was refused, closed so they are safe as a label.
const (
	authRejectMissing   = "missing"
	authRejectMalformed = "malformed"
	authRejectInvalid   = "invalid"
)

// schedulerAuthRejectedTotal counts RPCs refused for a missing or wrong token.
//
// agentenv_scheduler_rpc_duration_seconds also carries status=unauthenticated,
// but only for the RPCs it labels. This counts every refusal, and its reason
// separates a client that sends nothing — an old node, an unconfigured
// gateway — from one that sends the wrong secret.
var schedulerAuthRejectedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agentenv_scheduler_auth_rejected_total",
		Help: "Scheduler RPCs refused as unauthenticated, by reason.",
	},
	[]string{"reason"},
)

// ResolveAuthToken returns the shared token from the configured literal or
// file, or nil when neither is set.
//
// A file that is configured but empty or unreadable is an error, not "auth
// off": the operator meant to enforce, and silently opening the listener
// because a mount is missing is the failure a startup check exists to catch.
func ResolveAuthToken(token string, file string) ([]byte, error) {
	token = strings.TrimSpace(token)
	file = strings.TrimSpace(file)
	if token != "" && file != "" {
		return nil, errors.New("scheduler.auth_token and scheduler.auth_token_file are both set; configure one")
	}
	if file != "" {
		f, err := os.Open(file)
		if err != nil {
			return nil, fmt.Errorf("read scheduler auth token file: %w", err)
		}
		defer f.Close()
		contents, err := io.ReadAll(io.LimitReader(f, maxAuthTokenLen+1))
		if err != nil {
			return nil, fmt.Errorf("read scheduler auth token file %s: %w", file, err)
		}
		if len(contents) > maxAuthTokenLen {
			return nil, fmt.Errorf("scheduler auth token file %s exceeds %d bytes", file, maxAuthTokenLen)
		}
		token = strings.TrimSpace(string(contents))
		if token == "" {
			return nil, fmt.Errorf("scheduler auth token file %s is empty", file)
		}
	}
	if token == "" {
		return nil, nil
	}
	if len(token) > maxAuthTokenLen {
		return nil, fmt.Errorf("scheduler auth token exceeds %d bytes", maxAuthTokenLen)
	}
	return []byte(token), nil
}

// authenticator checks one RPC's metadata against the configured token.
type authenticator struct {
	// digest is the SHA-256 of the token. Comparing digests rather than the
	// tokens themselves keeps the comparison constant-time in the token's
	// length as well as its contents: subtle.ConstantTimeCompare returns
	// early on a length mismatch, and two digests always have the same length.
	digest [sha256.Size]byte
}

func newAuthenticator(token []byte) *authenticator {
	if len(token) == 0 {
		return nil
	}
	return &authenticator{digest: sha256.Sum256(token)}
}

// check returns nil for an RPC that may proceed.
func (a *authenticator) check(ctx context.Context, fullMethod string) error {
	if a == nil || strings.HasPrefix(fullMethod, healthServicePrefix) {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return a.reject(authRejectMissing)
	}
	values := md.Get(AuthMetadataKey)
	if len(values) == 0 {
		return a.reject(authRejectMissing)
	}
	// One credential per request. Accepting the first valid one of several
	// would let a client probe with many guesses per RPC.
	if len(values) != 1 {
		return a.reject(authRejectMalformed)
	}
	scheme, presented, found := strings.Cut(strings.TrimSpace(values[0]), " ")
	if !found || !strings.EqualFold(scheme, AuthBearerScheme) {
		return a.reject(authRejectMalformed)
	}
	presented = strings.TrimSpace(presented)
	if presented == "" {
		return a.reject(authRejectMalformed)
	}
	got := sha256.Sum256([]byte(presented))
	if subtle.ConstantTimeCompare(got[:], a.digest[:]) != 1 {
		return a.reject(authRejectInvalid)
	}
	return nil
}

func (a *authenticator) reject(reason string) error {
	schedulerAuthRejectedTotal.WithLabelValues(reason).Inc()
	// One message for every reason. Telling the caller which check failed
	// tells an attacker the same thing; the reason is in the metric.
	return status.Error(codes.Unauthenticated, "scheduler requires a valid bearer token")
}

// AuthUnaryInterceptor refuses every unary RPC that does not carry the shared
// token, except health checks. A nil or empty token disables enforcement and
// accepts every call, which is the shape a rollout needs: the scheduler can
// be upgraded and the token distributed to nodes and gateways before
// enforcement is switched on, and switched off again without a redeploy of
// either.
func AuthUnaryInterceptor(token []byte) grpc.UnaryServerInterceptor {
	auth := newAuthenticator(token)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := auth.check(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// AuthStreamInterceptor is AuthUnaryInterceptor for streaming RPCs. The
// scheduler serves none today; this exists so that adding one cannot open a
// path around the token.
func AuthStreamInterceptor(token []byte) grpc.StreamServerInterceptor {
	auth := newAuthenticator(token)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := auth.check(ss.Context(), info.FullMethod); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// AuthEnabled reports whether a token would be enforced.
func AuthEnabled(token []byte) bool {
	return len(token) > 0
}
