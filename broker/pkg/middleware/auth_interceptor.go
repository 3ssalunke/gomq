package middleware

import (
	"context"
	"errors"
	"strings"

	"github.com/3ssalunke/gomq/broker/internal/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func getApiKeyFromMetadata(md metadata.MD) string {
	if values := md["authorization"]; len(values) > 0 {
		authHeader := values[0]
		if strings.HasPrefix(authHeader, "Bearer ") {
			return strings.TrimPrefix(authHeader, "Bearer ")
		}
	}

	return ""
}

func ApiKeyAuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("missing metadata")
	}

	apiKey := getApiKeyFromMetadata(md)
	if apiKey == "" {
		return nil, errors.New("api key missing")
	}

	_, err := auth.ValidateApiKey(apiKey)
	if err != nil {
		return nil, err
	}

	return handler(ctx, req)
}
