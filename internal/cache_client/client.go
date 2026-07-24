package cache_client

import (
	"fmt"
	"strconv"

	"github.com/AndrewK4758/shared_utils/envutil"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewClient(addr string) (pb.SemanticCacheServiceClient, *grpc.ClientConn, error) {
	if addr == "" {
		addr = "localhost:50055"
	}

	maxMsgSizeMb := 100
	if envVal := envutil.GetStringOrDefault("GRPC_MAX_MESSAGE_SIZE_MB", ""); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			maxMsgSizeMb = parsed
		}
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSizeMb*1024*1024)))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to semantic cache service at %s: %w", addr, err)
	}

	return pb.NewSemanticCacheServiceClient(conn), conn, nil
}
