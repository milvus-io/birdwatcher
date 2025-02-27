package common

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func WriteChannelWatchInfo(ctx context.Context, cli kv.MetaKV, basePath string, info *models.ChannelWatch, schema *schemapb.CollectionSchema) error {
	pb := info.GetProto()
	bs, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	err = cli.Save(ctx, info.Key(), string(bs))
	return err
}
