package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// ListIndex list all index with all filter satified.
func ListIndex(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(index *models.FieldIndex) bool) ([]*models.FieldIndex, error) {
	prefix := path.Join(basePath, IndexPrefix) + "/"
	return ListObj2Models(ctx, cli, prefix, models.NewProtoWrapper[*indexpb.FieldIndex], filters...)
}

func ListSegmentIndex(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(segIdx *models.SegmentIndex) bool) ([]*models.SegmentIndex, error) {
	prefix := path.Join(basePath, SegmentIndexPrefix) + "/"
	return ListObj2Models(ctx, cli, prefix, models.NewProtoWrapper[*indexpb.SegmentIndex], filters...)
}
