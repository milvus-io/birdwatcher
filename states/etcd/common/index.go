package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/indexpb"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ListIndex list all index with all filter satified.
func ListIndex(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(index *indexpb.IndexMeta) bool) ([]indexpb.IndexMeta, error) {
	prefix := path.Join(basePath, "indexes") + "/"
	result, _, err := ListProtoObjects(ctx, cli, prefix, filters...)
	return result, err
}

// ListSegmentIndex list segment index info.
func ListSegmentIndex(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(segIdx *etcdpb.SegmentIndexInfo) bool) ([]etcdpb.SegmentIndexInfo, error) {
	prefix := path.Join(basePath, "root-coord/segment-index") + "/"
	result, _, err := ListProtoObjects(ctx, cli, prefix, filters...)
	return result, err
}
