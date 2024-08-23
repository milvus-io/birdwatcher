package common

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/internalpb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	msgpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/msgpb"
	schemapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/schemapb"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ListChannelWatchV1 list v2.1 channel watch info meta.
func ListChannelWatchV1(cli kv.MetaKV, basePath string, filters ...func(channel *datapb.ChannelWatchInfo) bool) ([]datapb.ChannelWatchInfo, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "channelwatch") + "/"
	return ListProtoObjects(ctx, cli, prefix, filters...)
}

// ListChannelWatchV2 lists v2.2 channel watch info meta.
func ListChannelWatchV2(cli kv.MetaKV, basePath string, filters ...func(channel *datapbv2.ChannelWatchInfo) bool) ([]datapbv2.ChannelWatchInfo, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "channelwatch") + "/"
	return ListProtoObjects(ctx, cli, prefix, filters...)
}

func ListChannelCheckpint(cli kv.MetaKV, basePath string, filters ...func(pos *internalpb.MsgPosition) bool) ([]internalpb.MsgPosition, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "datacoord-meta", "channel-cp") + "/"
	return ListProtoObjects(ctx, cli, prefix, filters...)
}

// ListChannelWatch lists channel watch info meta.
func ListChannelWatch(ctx context.Context, cli kv.MetaKV, basePath string, version string, filters ...func(*models.ChannelWatch) bool) ([]*models.ChannelWatch, error) {
	prefix := path.Join(basePath, "channelwatch") + "/"
	var result []*models.ChannelWatch
	switch version {
	case models.LTEVersion2_1:
		infos, paths, err := ListProtoObjects[datapb.ChannelWatchInfo](ctx, cli, prefix)
		if err != nil {
			return nil, err
		}
		result = lo.Map(infos, func(info datapb.ChannelWatchInfo, idx int) *models.ChannelWatch {
			return models.GetChannelWatchInfo[*datapb.ChannelWatchInfo, datapb.ChannelWatchState, *datapb.VchannelInfo, *internalpb.MsgPosition](&info, paths[idx])
		})
	case models.GTEVersion2_2:
		infos, paths, err := ListProtoObjects[datapbv2.ChannelWatchInfo](ctx, cli, prefix)
		if err != nil {
			return nil, err
		}
		result = lo.Map(infos, func(info datapbv2.ChannelWatchInfo, idx int) *models.ChannelWatch {
			return models.GetChannelWatchInfoV2[*datapbv2.ChannelWatchInfo, datapbv2.ChannelWatchState, *datapbv2.VchannelInfo, *msgpbv2.MsgPosition](&info, paths[idx])
		})
	default:
		return nil, errors.New("version not supported")
	}
	result = lo.Filter(result, func(info *models.ChannelWatch, _ int) bool {
		for _, filter := range filters {
			if !filter(info) {
				return false
			}
		}
		return true
	})
	return result, nil
}

func SetChannelWatch(ctx context.Context, cli kv.MetaKV, basePath string, channelName string, collection *models.Collection) error {
	removelKey := path.Join(basePath, "datacoord-meta/channel-removal", channelName)
	err := cli.Save(ctx, removelKey, "non-removed")
	if err != nil {
		return err
	}

	watchKey := path.Join(basePath, "channelwatch", fmt.Sprintf("%d", math.MinInt64), channelName)

	info := &datapbv2.ChannelWatchInfo{
		Vchan: &datapbv2.VchannelInfo{
			CollectionID: collection.ID,
			ChannelName:  channelName,
			//		SeekPosition: collection.Sc

		},
		Schema: &schemapbv2.CollectionSchema{
			Name: collection.Schema.Name,
			Fields: lo.Map(collection.Schema.Fields, func(field models.FieldSchema, idx int) *schemapbv2.FieldSchema {
				return &schemapbv2.FieldSchema{
					FieldID:     field.FieldID,
					Name:        field.Name,
					Description: field.Description,
					DataType:    schemapbv2.DataType(field.DataType),
					TypeParams: lo.MapToSlice(field.Properties, func(key, value string) *commonpb.KeyValuePair {
						return &commonpb.KeyValuePair{
							Key:   key,
							Value: value,
						}
					}),
				}
			}),
		},
	}

	bs, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	err = cli.Save(ctx, watchKey, string(bs))
	return err
}
