package common

import (
	"context"
	"path"
	"time"

	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/indexpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ListIndex list all index with all filter satified.
func ListIndex(cli *clientv3.Client, basePath string, filters ...func(index *indexpb.IndexMeta) bool) ([]indexpb.IndexMeta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	prefix := path.Join(basePath, "indexes") + "/"

	result, _, err := ListProtoObjects(ctx, cli, prefix, filters...)
	return result, err
}

// ListSegmentIndex list segment index info.
func ListSegmentIndex(cli *clientv3.Client, basePath string, filters ...func(segIdx *etcdpb.SegmentIndexInfo) bool) ([]etcdpb.SegmentIndexInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "root-coord/segment-index") + "/"
	result, _, err := ListProtoObjects(ctx, cli, prefix, filters...)
	return result, err
}
