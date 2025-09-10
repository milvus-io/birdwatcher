package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

const (
	walBroadcastPrefix = "streamingcoord-meta/broadcast-task/"
)

// ListWalBroadcast list broadcast tasks.
func ListWalBroadcast(ctx context.Context, cli kv.MetaKV, basePath string) ([]*streamingpb.BroadcastTask, error) {
	prefix := path.Join(basePath, walBroadcastPrefix) + "/"
	metas, _, err := ListProtoObjects[streamingpb.BroadcastTask](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}
	return metas, nil
}
