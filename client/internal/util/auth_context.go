package util

import (
	"context"

	"google.golang.org/grpc/metadata"
)

func GetAuthContext(ctx context.Context, apiKey string) context.Context {
	md := metadata.New(map[string]string{"authorization": "Bearer " + apiKey})

	return metadata.NewOutgoingContext(ctx, md)
}
