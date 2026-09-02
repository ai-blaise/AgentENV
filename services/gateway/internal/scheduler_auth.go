package gateway

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// The scheduler's listener accepts RecordAssignment, which rewrites any
// sandbox's routing, from anyone who can reach it. The shared token is what
// lets the scheduler tell a gateway from anything else on the network. It
// travels as `authorization: Bearer <token>`, the one metadata key both sides
// agree on; the scheduler compares it in constant time and rejects everything
// but the health service without it.
const (
	schedulerAuthMetadataKey = "authorization"
	schedulerAuthScheme      = "Bearer "
)

// schedulerBearerToken presents one token on every RPC of a connection.
type schedulerBearerToken string

func (t schedulerBearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{schedulerAuthMetadataKey: schedulerAuthScheme + string(t)}, nil
}

// RequireTransportSecurity is false because the scheduler link is plaintext
// inside the cluster today, and grpc refuses to send credentials over an
// insecure connection that demand otherwise. The token guards the scheduler
// against forged writes from the network it is already on; it is not a
// secret from that network's transport.
func (schedulerBearerToken) RequireTransportSecurity() bool { return false }

// SchedulerDialOptions returns the options every gateway connection to a
// scheduler dials with. An empty token dials exactly as before the token
// existed, which is what a deployment that has not rolled the scheduler side
// yet needs.
func SchedulerDialOptions(token string) []grpc.DialOption {
	if token == "" {
		return nil
	}
	return []grpc.DialOption{grpc.WithPerRPCCredentials(schedulerBearerToken(token))}
}

// Compile-time proof the type satisfies the interface grpc dials with.
var _ credentials.PerRPCCredentials = schedulerBearerToken("")
