package remove

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ChannelCommand returns remove channel command.
func ChannelCommand(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "channel",
		Short: "Remove channel from datacoord meta with specified condition if orphan",
		Run: func(cmd *cobra.Command, args []string) {
			channelName, err := cmd.Flags().GetString("channel")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			var collections []etcdpb.CollectionInfo

			colls, err := common.ListCollections(cli, basePath, func(info *etcdpb.CollectionInfo) bool {
				return true
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			collections = append(collections, colls...)

			if len(collections) == 0 {
				fmt.Println("no collection found")
				return
			}

			validChannels := make(map[string]struct{})
			for _, collection := range collections {
				for _, vchan := range collection.GetVirtualChannelNames() {
					validChannels[vchan] = struct{}{}
				}
			}

			watchChannels, paths, err := common.ListChannelWatchV2(cli, basePath, func(info *datapbv2.ChannelWatchInfo) bool {
				if len(channelName) > 0 {
					return info.GetVchan().GetChannelName() == channelName
				}
				return true
			})

			targets := make([]string, 0, len(paths))
			for i, watchChannel := range watchChannels {
				_, ok := validChannels[watchChannel.GetVchan().GetChannelName()]
				if !ok {
					fmt.Printf("%s might be an orphan channel, collection id: %d\n", watchChannel.GetVchan().GetChannelName(), watchChannel.GetVchan().GetCollectionID())
					targets = append(targets, paths[i])
				}
			}

			if !run {
				return
			}
			fmt.Printf("Start to delete orphan watch channel info...")
			for _, path := range paths {
				cli.Delete(context.Background(), path)
			}
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to remove segment from meta")
	cmd.Flags().String("channel", "", "channel name to remove")
	return cmd
}
