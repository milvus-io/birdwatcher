package show

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/internalpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// CheckpointCommand returns show checkpoint command.
func CheckpointCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "checkpoint",
		Short:   "list checkpoint collection vchannels",
		Aliases: []string{"checkpoints", "cp"},
		Run: func(cmd *cobra.Command, args []string) {

			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			coll, err := common.GetCollectionByIDVersion(context.Background(), cli, basePath, etcdversion.GetVersion(), collID)
			if err != nil {
				fmt.Println("failed to get collection", err.Error())
				return
			}

			for _, channel := range coll.Channels {
				var cp *internalpb.MsgPosition
				var segmentID int64
				var err error
				cp, err = getChannelCheckpoint(cli, basePath, channel.VirtualName)

				if err != nil {
					cp, segmentID, err = getCheckpointFromSegments(cli, basePath, collID, channel.VirtualName)
				}

				if cp == nil {
					fmt.Printf("vchannel %s position nil\n", channel.VirtualName)
				} else {
					t, _ := utils.ParseTS(cp.GetTimestamp())
					fmt.Printf("vchannel %s seek to %v, cp channel: %s", channel.VirtualName, t, cp.ChannelName)
					if segmentID > 0 {
						fmt.Printf(", for segment ID:%d\n", segmentID)
					} else {
						fmt.Printf(", from channel checkpoint\n")
					}
				}
			}
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	return cmd
}

func getChannelCheckpoint(cli clientv3.KV, basePath string, channelName string) (*internalpb.MsgPosition, error) {
	prefix := path.Join(basePath, "datacoord-meta", "channel-cp", channelName)
	results, _, err := common.ListProtoObjects[internalpb.MsgPosition](context.Background(), cli, prefix)
	if err != nil {
		return nil, err
	}

	if len(results) != 1 {
		return nil, fmt.Errorf("expected 1 position but got %d", len(results))
	}

	return &results[0], nil
}

func getCheckpointFromSegments(cli clientv3.KV, basePath string, collID int64, vchannel string) (*internalpb.MsgPosition, int64, error) {
	segments, err := common.ListSegments(cli, basePath, func(info *datapb.SegmentInfo) bool {
		return info.CollectionID == collID && info.InsertChannel == vchannel
	})
	if err != nil {
		fmt.Printf("fail to list segment for channel %s, err: %s\n", vchannel, err.Error())
		return nil, 0, err
	}
	fmt.Printf("find segments to list checkpoint for %s, segment found %d\n", vchannel, len(segments))
	var segmentID int64
	var pos *internalpb.MsgPosition
	for _, segment := range segments {
		if segment.State != commonpb.SegmentState_Flushed &&
			segment.State != commonpb.SegmentState_Growing &&
			segment.State != commonpb.SegmentState_Flushing {
			continue
		}
		// skip all empty segment
		if segment.GetDmlPosition() == nil && segment.GetStartPosition() == nil {
			continue
		}
		var segPos *internalpb.MsgPosition

		if segment.GetDmlPosition() != nil {
			segPos = segment.GetDmlPosition()
		} else {
			segPos = segment.GetStartPosition()
		}

		if pos == nil || segPos.GetTimestamp() < pos.GetTimestamp() {
			pos = segPos
			segmentID = segment.GetID()
		}
	}

	return pos, segmentID, nil
}
