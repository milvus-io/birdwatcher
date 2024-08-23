package common

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/models"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	schemapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/schemapb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func WriteChannelWatchInfo(ctx context.Context, cli clientv3.KV, basePath string, info *models.ChannelWatch, schema *schemapbv2.CollectionSchema) error {
	pb := &datapbv2.ChannelWatchInfo{
		Vchan:     info.VchanV2Pb,
		StartTs:   info.StartTs,
		State:     datapbv2.ChannelWatchState(info.State),
		TimeoutTs: info.TimeoutTs,
		Schema:    schema, // use passed schema
		Progress:  info.Progress,
		OpID:      info.OpID,
	}
	bs, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	_, err = cli.Put(ctx, info.Key(), string(bs))
	return err
}
