package common

import (
	"context"
	"path"
	"time"

	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ListChannelWatchV1 list v2.1 channel watch info meta.
func ListChannelWatchV1(cli *clientv3.Client, basePath string, filters ...func(channel *datapb.ChannelWatchInfo) bool) ([]datapb.ChannelWatchInfo, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "channelwatch") + "/"
	return ListProtoObjects(ctx, cli, prefix, filters...)
}
