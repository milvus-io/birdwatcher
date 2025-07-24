package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

const (
	walDistributionPrefix = "streamingcoord-meta/pchannel/"
)

func ListWALDistribution(ctx context.Context, cli kv.MetaKV, basePath string, pchannel string) ([]*streamingpb.PChannelMeta, error) {
	var prefix string
	if len(pchannel) > 0 {
		prefix = path.Join(basePath, walDistributionPrefix, pchannel)
	} else {
		prefix = path.Join(basePath, walDistributionPrefix) + "/"
	}

	metas, _, err := ListProtoObjects[streamingpb.PChannelMeta](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}
	return metas, nil
}
