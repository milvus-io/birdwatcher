package common

import (
	"context"
	"fmt"
	"math"
	"path"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func ListChannelCheckpoint(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(pos *models.MsgPosition) bool) ([]*models.MsgPosition, error) {
	prefix := path.Join(basePath, DCPrefix, ChannelCheckpointPrefix) + "/"
	// return ListProtoObjects(ctx, cli, prefix, filters...)
	return ListObj2Models(ctx, cli, prefix, models.NewProtoWrapper[*msgpb.MsgPosition], filters...)
}

// ListChannelWatch lists channel watch info meta.
func ListChannelWatch(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*models.ChannelWatch) bool) ([]*models.ChannelWatch, error) {
	prefix := path.Join(basePath, "channelwatch") + "/"

	return ListObj2Models(ctx, cli, prefix, models.NewChannelWatch, filters...)
}

func SetChannelWatch(ctx context.Context, cli kv.MetaKV, basePath string, channelName string, col *models.Collection) error {
	collection := col.GetProto()
	removelKey := path.Join(basePath, DCPrefix, ChannelRemovalPrefix, channelName)
	err := cli.Save(ctx, removelKey, "non-removed")
	if err != nil {
		return err
	}

	watchKey := path.Join(basePath, ChannelWatchPrefix, fmt.Sprintf("%d", math.MinInt64), channelName)

	info := &datapb.ChannelWatchInfo{
		Vchan: &datapb.VchannelInfo{
			CollectionID: collection.GetID(),
			ChannelName:  channelName,
			//		SeekPosition: collection.Sc
		},
		Schema: collection.GetSchema(),
	}

	bs, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	err = cli.Save(ctx, watchKey, string(bs))
	return err
}
