package show

import (
	"context"
	"fmt"
	"path"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
)

const (
	unsubscribeChannelInfoPrefix = "queryCoord-unsubscribeChannelInfo"
)

func printNodeUnsubChannelInfos(infos []*querypb.UnsubscribeChannelInfo) {
	var collectionIDs []int64
	collectionMap := make(map[int64][]*querypb.UnsubscribeChannelInfo)
	for _, info := range infos {
		channels := info.GetCollectionChannels()
		if len(channels) <= 0 {
			continue
		}
		collectionID := channels[0].GetCollectionID()
		collectionIDs = append(collectionIDs, collectionID)
		sliceInfo := collectionMap[collectionID]
		sliceInfo = append(sliceInfo, info)
		collectionMap[collectionID] = sliceInfo
	}

	sort.Slice(collectionIDs, func(i, j int) bool {
		return collectionIDs[i] < collectionIDs[j]
	})

	for _, colID := range collectionIDs {
		sliceInfos := collectionMap[colID]
		for _, info := range sliceInfos {
			fmt.Printf("%s\n", info.String())
		}
	}
}

func listQueryCoordUnsubChannelInfos(cli kv.MetaKV, basePath string) ([]*querypb.UnsubscribeChannelInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	prefix := path.Join(basePath, unsubscribeChannelInfoPrefix)

	_, vals, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	ret := make([]*querypb.UnsubscribeChannelInfo, 0, len(vals))

	for _, val := range vals {
		channelInfo := &querypb.UnsubscribeChannelInfo{}
		err = proto.Unmarshal([]byte(val), channelInfo)
		if err != nil {
			return nil, err
		}
		ret = append(ret, channelInfo)
	}
	return ret, nil
}

func printDMChannelWatchInfo(infos []*querypb.DmChannelWatchInfo) {
	common.SortByCollection(infos)
	for _, info := range infos {
		// TODO beautify output
		fmt.Println(info.String())
	}
}

func printDeltaChannelInfos(infos []*datapb.VchannelInfo) {
	common.SortByCollection(infos)
	for _, info := range infos {
		// TODO beautify output
		fmt.Println(info.String())
	}
}

// QueryCoordChannelCommand returns show querycoord-channel command.
func QueryCoordChannelCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "querycoord-channel",
		Short:   "display querynode information from querycoord cluster",
		Aliases: []string{"querycoord-channels"},
		RunE: func(cmd *cobra.Command, args []string) error {
			taskType, err := cmd.Flags().GetString("type")
			if err != nil {
				return err
			}

			if taskType != "" && taskType != "all" && taskType != "dml" && taskType != "delta" && taskType != "unsub" {
				fmt.Println("wrong channel type")
				return nil
			}

			unsubInfos, err := listQueryCoordUnsubChannelInfos(cli, basePath)
			if err != nil {
				return err
			}

			if taskType == "" || taskType == "all" || taskType == "unsub" {
				printNodeUnsubChannelInfos(unsubInfos)
			}

			dmWatchInfo, err := common.ListQueryCoordDMLChannelInfos(cli, basePath)
			if err != nil {
				return err
			}
			if taskType == "" || taskType == "all" || taskType == "dml" {
				printDMChannelWatchInfo(dmWatchInfo)
			}
			deltaChannels, err := common.ListQueryCoordDeltaChannelInfos(cli, basePath)
			if err != nil {
				return err
			}
			if taskType == "" || taskType == "all" || taskType == "delta" {
				printDeltaChannelInfos(deltaChannels)
			}
			return nil
		},
	}
	cmd.Flags().String("type", "all", "filter channel types [dml, delta, unsub, all]")
	return cmd
}
