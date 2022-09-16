// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package states

import (
	"context"
	"fmt"
	"path"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
)

const (
	unsubscribeChannelInfoPrefix = "queryCoord-unsubscribeChannelInfo"
	dmChannelMetaPrefix          = "queryCoord-dmChannelWatchInfo"
	deltaChannelMetaPrefix       = "queryCoord-deltaChannel"
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

func listQueryCoordUnsubChannelInfos(cli *clientv3.Client, basePath string) ([]*querypb.UnsubscribeChannelInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	prefix := path.Join(basePath, unsubscribeChannelInfoPrefix)

	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	ret := make([]*querypb.UnsubscribeChannelInfo, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		channelInfo := &querypb.UnsubscribeChannelInfo{}
		err = proto.Unmarshal(kv.Value, channelInfo)
		if err != nil {
			return nil, err
		}
		ret = append(ret, channelInfo)
	}
	return ret, nil
}

func printDMChannelWatchInfo(infos []*querypb.DmChannelWatchInfo) {
	var collectionIDs []int64
	collectionMap := make(map[int64][]*querypb.DmChannelWatchInfo)
	for _, info := range infos {
		collectionID := info.GetCollectionID()
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

func listQueryCoordDMLChannelInfos(cli *clientv3.Client, basePath string) ([]*querypb.DmChannelWatchInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	prefix := path.Join(basePath, dmChannelMetaPrefix)

	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	ret := make([]*querypb.DmChannelWatchInfo, 0)
	for _, kv := range resp.Kvs {
		channelInfo := &querypb.DmChannelWatchInfo{}
		err = proto.Unmarshal(kv.Value, channelInfo)
		if err != nil {
			return nil, err
		}
		ret = append(ret, channelInfo)
	}
	return ret, nil
}

func listQueryCoordDeltaChannelInfos(cli *clientv3.Client, basePath string) ([]*datapb.VchannelInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	prefix := path.Join(basePath, deltaChannelMetaPrefix)

	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	ret := make([]*datapb.VchannelInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		channelInfo := &datapb.VchannelInfo{}
		err = proto.Unmarshal(kv.Value, channelInfo)
		if err != nil {
			return nil, err
		}
		reviseVChannelInfo(channelInfo)
		ret = append(ret, channelInfo)
	}
	return ret, nil
}

func printDeltaChannelInfos(infos []*datapb.VchannelInfo) {
	var collectionIDs []int64
	collectionMap := make(map[int64][]*datapb.VchannelInfo)
	for _, info := range infos {
		collectionID := info.GetCollectionID()
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

func getQueryCoordChannelInfoCmd(cli *clientv3.Client, basePath string) *cobra.Command {
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

			dmWatchInfo, err := listQueryCoordDMLChannelInfos(cli, basePath)
			if err != nil {
				return err
			}
			if taskType == "" || taskType == "all" || taskType == "dml" {
				printDMChannelWatchInfo(dmWatchInfo)
			}
			deltaChannels, err := listQueryCoordDeltaChannelInfos(cli, basePath)
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
