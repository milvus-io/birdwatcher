package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

const (
	replicateConfiguration = "streamingcoord-meta/replicate-configuration"
	replicatePChannel      = "streamingcoord-meta/replicating-pchannel/"
)

func ListReplicateConfiguration(ctx context.Context, cli kv.MetaKV, basePath string) (*streamingpb.ReplicateConfigurationMeta, error) {
	prefix := path.Join(basePath, replicateConfiguration)
	metas, _, err := ListProtoObjects[streamingpb.ReplicateConfigurationMeta](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}
	return metas[0], nil
}

func ListReplicatePChannel(ctx context.Context, cli kv.MetaKV, basePath string) ([]*streamingpb.ReplicatePChannelMeta, error) {
	prefix := path.Join(basePath, replicatePChannel) + "/"
	metas, _, err := ListProtoObjects[streamingpb.ReplicatePChannelMeta](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}
	return metas, nil
}
