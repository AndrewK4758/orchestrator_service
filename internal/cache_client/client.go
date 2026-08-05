package cache_client

import (
	"fmt"

	"github.com/AndrewK4758/shared_utils/envutil"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewClient(addr string) (pb.SemanticCacheServiceClient, *grpc.ClientConn, error) {

	maxMsgSizeMb := envutil.MustGetInt("GRPC_MAX_MESSAGE_SIZE_MB")
	if maxMsgSizeMb <= 0 {
		panic(fmt.Errorf("[Context: cache_client/NewCacheClient] Invalid maxMsgSizeMb: %d, must be > 0", maxMsgSizeMb))
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSizeMb*1024*1024)))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to semantic cache service at %s: %w", addr, err)
	}

	return pb.NewSemanticCacheServiceClient(conn), conn, nil
}
