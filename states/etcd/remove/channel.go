package remove

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/proto/v2.0/internalpb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

// ChannelCommand returns remove channel command.
func ChannelCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "channel",
		Short: "Remove channel watchinfo or checkpoint from datacoord meta with specified condition if orphan",
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
			removeCp, err := cmd.Flags().GetBool("orphan-checkpoints")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			if force && removeCp && len(channelName) == 0 {
				fmt.Println("Attmept to force remove all checkpoints, ignore it...")
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

			targets := make([]string, 0)
			if removeCp {
				allCps, cpPaths, err := common.ListChannelCheckpint(cli, basePath, func(pos *internalpb.MsgPosition) bool {
					if len(channelName) > 0 {
						return pos.GetChannelName() == channelName
					}
					return true
				})
				if err != nil {
					fmt.Println(err.Error())
					return
				}

				for i, orphanCp := range allCps {
					_, ok := validChannels[orphanCp.GetChannelName()]
					if !ok || force {
						fmt.Printf("%s selected as target orpah checkpoint, path: %s\n", orphanCp.GetChannelName(), cpPaths[i])
						targets = append(targets, cpPaths[i])
					}
				}
			} else {
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

				for i, watchChannel := range watchChannels {
					_, ok := validChannels[watchChannel.GetVchan().GetChannelName()]
					if !ok || force {
						fmt.Printf("%s selected as target channel watchinfo, collection id: %d\n", watchChannel.GetVchan().GetChannelName(), watchChannel.GetVchan().GetCollectionID())
						targets = append(targets, paths[i])
					}
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
	cmd.Flags().Bool("orphan-checkpoints", false, "Whether to remove leaking channel checkpoints, cannot be used with --force when no channel is specified. If no channel is specified, all orphan checkpoints will be removed")
	return cmd
}
