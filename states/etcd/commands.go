package etcd

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/states/etcd/repair"
	"github.com/milvus-io/birdwatcher/states/etcd/show"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ShowCommand returns sub command for instanceState.
// show [subCommand] [options...]
// sub command [collection|session|segment]
func ShowCommand(cli *clientv3.Client, basePath string) *cobra.Command {
	showCmd := &cobra.Command{
		Use: "show",
	}

	showCmd.AddCommand(
		// show collection
		show.CollectionCommand(cli, basePath),
		// show sessions
		show.SessionCommand(cli, basePath),
		// show segments
		show.SegmentCommand(cli, basePath),
		// show segment-loaded
		show.SegmentLoadedCommand(cli, basePath),
		// show index
		show.IndexCommand(cli, basePath),
		// show segment-index
		show.SegmentIndexCommand(cli, basePath),

		// show replica
		show.ReplicaCommand(cli, basePath),
		// show checkpoint
		show.CheckpointCommand(cli, basePath),
		// show channel-watched
		show.ChannelWatchedCommand(cli, basePath),

		// show collection-loaded
		show.CollectionLoadedCommand(cli, basePath),

		// v2.1 legacy commands
		// show querycoord-tasks
		show.QueryCoordTasks(cli, basePath),
		// show querycoord-channels
		show.QueryCoordChannelCommand(cli, basePath),
		// show querycoord-cluster
		show.QueryCoordClusterCommand(cli, basePath),
	)
	return showCmd
}

// RepairCommand returns etcd repair commands.
func RepairCommand(cli *clientv3.Client, basePath string) *cobra.Command {
	repairCmd := &cobra.Command{
		Use: "repair",
	}

	repairCmd.AddCommand(
		// repair segment
		repair.SegmentCommand(cli, basePath),
		// repair channel
		repair.ChannelCommand(cli, basePath),
		// repair checkpoint
		repair.CheckpointCommand(cli, basePath),
		// repair empty-segment
		repair.EmptySegmentCommand(cli, basePath),
	)

	return repairCmd
}

// RawCommands provides raw "get" command to list kv in etcd
func RawCommands(cli *clientv3.Client) []*cobra.Command {
	cmd := &cobra.Command{
		Use: "get",
		Run: func(cmd *cobra.Command, args []string) {
			for _, arg := range args {
				fmt.Println("list wrth", arg)
				resp, err := cli.Get(context.Background(), arg, clientv3.WithPrefix())
				if err != nil {
					fmt.Println(err.Error())
					continue
				}
				for _, kv := range resp.Kvs {
					fmt.Printf("key: %s\n", string(kv.Key))
					fmt.Printf("Value: %s\n", string(kv.Value))
				}
			}
		},
	}

	return []*cobra.Command{cmd}
}

/*
func removeSegmentByID(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-segment-by-id",
		Short: "Remove segment from meta with specified segment id",
		RunE: func(cmd *cobra.Command, args []string) error {
			targetSegmentID, err := cmd.Flags().GetInt64("segment")
			if err != nil {
				return err
			}
			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				return err
			}
			segments, err := listSegments(cli, basePath, func(info *datapb.SegmentInfo) bool {
				return true
			})
			if err != nil {
				fmt.Println("failed to list segments", err.Error())
				return nil
			}

			for _, info := range segments {
				if info.GetID() == targetSegmentID {
					fmt.Printf("target segment %d found:\n", info.GetID())
					printSegmentInfo(info, false)
					if run {
						err := removeSegment(cli, basePath, info)
						if err == nil {
							fmt.Printf("remove segment %d from meta succeed\n", info.GetID())
						} else {
							fmt.Printf("remove segment %d failed, err: %s\n", info.GetID(), err.Error())
						}
						return nil
					}
					if !isEmptySegment(info) {
						fmt.Printf("\n[WARN] segment %d is not empty, please make sure you know what you're doing\n", targetSegmentID)
					}
					return nil
				}
			}
			fmt.Printf("[WARN] cannot find segment %d\n", targetSegmentID)
			return nil
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to remove segment from meta")
	cmd.Flags().Int64("segment", 0, "segment id to remove")
	return cmd
}*/
