package middleware

import (
	"context"
	"strings"

	"github.com/3ssalunke/gomq/broker/internal/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ContextUserKey string

const UserKey = ContextUserKey("user")

func getApiKeyFromMetadata(md metadata.MD) string {
	if values := md["authorization"]; len(values) > 0 {
		authHeader := values[0]
		if strings.HasPrefix(authHeader, "Bearer ") {
			return strings.TrimPrefix(authHeader, "Bearer ")
		}
	}

	return ""
}

func ApiKeyAuthInterceptor(auth *auth.Auth) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if strings.HasPrefix(info.FullMethod, "/gomq.broker.BrokerService/CreateAdmin") || strings.HasPrefix(info.FullMethod, "/gomq.cluster.ClusterSync/BroadCastMessageToPeer") || strings.HasPrefix(info.FullMethod, "/gomq.cluster.ClusterSync/SyncCluster") {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "missing auth metadata")
		}

		apiKey := getApiKeyFromMetadata(md)
		if apiKey == "" {
			return nil, status.Errorf(codes.Unauthenticated, "api key missing")
		}

		user, err := auth.ValidateApiKey(apiKey)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, err.Error())
		}

		ctx = context.WithValue(ctx, UserKey, user)

		return handler(ctx, req)
	}
}
