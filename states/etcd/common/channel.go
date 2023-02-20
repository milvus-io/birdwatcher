package common

import (
	"context"
	"errors"
	"path"
	"time"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/internalpb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	internalpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/internalpb"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ListChannelWatchV1 list v2.1 channel watch info meta.
func ListChannelWatchV1(cli clientv3.KV, basePath string, filters ...func(channel *datapb.ChannelWatchInfo) bool) ([]datapb.ChannelWatchInfo, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "channelwatch") + "/"
	return ListProtoObjects(ctx, cli, prefix, filters...)
}

// ListChannelWatchV2 lists v2.2 channel watch info meta.
func ListChannelWatchV2(cli clientv3.KV, basePath string, filters ...func(channel *datapbv2.ChannelWatchInfo) bool) ([]datapbv2.ChannelWatchInfo, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "channelwatch") + "/"
	return ListProtoObjects(ctx, cli, prefix, filters...)
}

// ListChannelWatch lists channel watch info meta.
func ListChannelWatch(ctx context.Context, cli clientv3.KV, basePath string, version string, filters ...func(*models.ChannelWatch) bool) ([]*models.ChannelWatch, error) {
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
			return models.GetChannelWatchInfo[*datapbv2.ChannelWatchInfo, datapbv2.ChannelWatchState, *datapbv2.VchannelInfo, *internalpbv2.MsgPosition](&info, paths[idx])

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
