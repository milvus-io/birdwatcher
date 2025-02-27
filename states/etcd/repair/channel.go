package repair

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type RepairChannelParam struct {
	framework.ParamBase `use:"repair channel" desc:"do channel watch change and try to repair"`
	Collection          int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	Run                 bool  `name:"run" default:"false" desc:"actual do repair"`
}

// RepairChannelCommand defines repair channel command.
func (c *ComponentRepair) RepairChannelCommand(ctx context.Context, p *RepairChannelParam) error {
	if p.Collection == 0 {
		fmt.Println("collection id not provided")
		return nil
	}

	coll, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, p.Collection)
	if err != nil {
		fmt.Println("collection not found")
		return nil
	}

	chans := make(map[string]struct{})
	for _, c := range coll.Channels() {
		chans[c.VirtualName] = struct{}{}
	}

	infos, err := common.ListChannelWatch(ctx, c.client, c.basePath)
	if err != nil {
		return errors.Wrap(err, "failed to list channel watch info")
	}

	for _, info := range infos {
		delete(chans, info.GetProto().GetVchan().GetChannelName())
	}

	for vchan := range chans {
		fmt.Println("orphan channel found", vchan)
	}
	if len(chans) == 0 {
		fmt.Println("no orphan channel found")
		return nil
	}
	if p.Run {
		vchannelNames := make([]string, 0, len(chans))
		for vchan := range chans {
			vchannelNames = append(vchannelNames, vchan)
		}
		doDatacoordWatch(ctx, c.client, c.basePath, p.Collection, vchannelNames)
	}
	return nil
}

func doDatacoordWatch(ctx context.Context, cli kv.MetaKV, basePath string, collectionID int64, vchannels []string) {
	sessions, err := common.ListSessions(ctx, cli, basePath)
	if err != nil {
		fmt.Println("failed to list session")
		return
	}

	for _, session := range sessions {
		if session.ServerName == "datacoord" {
			opts := []grpc.DialOption{
				grpc.WithInsecure(),
				grpc.WithBlock(),
				grpc.WithTimeout(2 * time.Second),
			}

			conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
			if err != nil {
				fmt.Printf("failed to connect to DataCoord(%d) addr: %s, err: %s\n", session.ServerID, session.Address, err.Error())
				return
			}

			client := datapb.NewDataCoordClient(conn)
			resp, err := client.WatchChannels(context.Background(), &datapb.WatchChannelsRequest{
				CollectionID: collectionID,
				ChannelNames: vchannels,
			})
			if err != nil {
				fmt.Println("failed to call WatchChannels", err.Error())
				return
			}

			if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				fmt.Println("WatchChannels failed", resp.GetStatus().GetErrorCode().String(), resp.GetStatus().GetReason())
				return
			}

			fmt.Printf("Invoke WatchChannels(%v) done\n", vchannels)
		}
	}
}
