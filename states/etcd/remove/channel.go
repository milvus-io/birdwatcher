package remove

import (
	"context"
	"fmt"
	"time"

	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ChannelCommand returns remove channel command.
func ChannelCommand(cli clientv3.KV, basePath string) *cobra.Command {
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
			force, err := cmd.Flags().GetBool("force")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			collections, err := common.ListCollectionsVersion(context.Background(), cli, basePath, etcdversion.GetVersion())
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			validChannels := make(map[string]struct{})
			for _, collection := range collections {
				for _, channel := range collection.Channels {
					validChannels[channel.VirtualName] = struct{}{}
				}
			}

			watchChannels, paths, err := common.ListChannelWatchV2(cli, basePath, func(info *datapbv2.ChannelWatchInfo) bool {
				if len(channelName) > 0 {
					return info.GetVchan().GetChannelName() == channelName
				}
				return true
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			targets := make([]string, 0, len(paths))
			for i, watchChannel := range watchChannels {
				_, ok := validChannels[watchChannel.GetVchan().GetChannelName()]
				if !ok || force {
					fmt.Printf("%s selected as target channel, collection id: %d\n", watchChannel.GetVchan().GetChannelName(), watchChannel.GetVchan().GetCollectionID())
					targets = append(targets, paths[i])
				}
			}

			if !run {
				return
			}
			fmt.Printf("Start to delete orphan watch channel info...\n")
			for _, path := range targets {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				_, err := cli.Delete(ctx, path)
				cancel()
				if err != nil {
					fmt.Printf("failed to remove watch key %s, error: %s\n", path, err.Error())
					continue
				}
				fmt.Printf("remove orphan channel %s done\n", path)
			}
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to remove channel from meta")
	cmd.Flags().String("channel", "", "channel name to remove")
	cmd.Flags().Bool("force", false, "force remove channel ignoring collection check")
	return cmd
}
