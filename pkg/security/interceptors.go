package security

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const metadataKeyAuthorization = "authorization"

// AuthInterceptor enforces static bearer tokens on unary RPCs, including
// RegisterWorker and Heartbeat. When validTokens is empty, all requests are allowed.
func AuthInterceptor(validTokens map[string]bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if len(validTokens) == 0 {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		vals := md.Get(metadataKeyAuthorization)
		if len(vals) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing authorization")
		}

		token := parseBearerToken(vals[0])
		if token == "" || !validTokens[token] {
			return nil, status.Error(codes.Unauthenticated, "invalid or unknown token")
		}

		return handler(ctx, req)
	}
}

func parseBearerToken(header string) string {
	h := strings.TrimSpace(header)
	if h == "" {
		return ""
	}
	parts := strings.SplitN(h, " ", 2)
	if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
		return strings.TrimSpace(parts[1])
	}
	return h
}
