package util

import (
	"context"

	"google.golang.org/grpc/metadata"
)

func GetAuthContext(ctx context.Context) context.Context {
	md := metadata.New(map[string]string{"authorization": "Bearer " + "admin"})

	return metadata.NewOutgoingContext(ctx, md)
}
