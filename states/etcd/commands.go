package etcd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/states/etcd/remove"
	"github.com/milvus-io/birdwatcher/states/etcd/repair"
	"github.com/milvus-io/birdwatcher/states/etcd/set"
	"github.com/milvus-io/birdwatcher/states/etcd/show"
)

// ShowCommand returns sub command for instanceState.
// show [subCommand] [options...]
// sub command [collection|session|segment]
func ShowCommand(cli clientv3.KV, basePath string) *cobra.Command {
	showCmd := &cobra.Command{
		Use: "show",
	}

	showCmd.AddCommand(
		// show collection
		show.CollectionCommand(cli, basePath),
		// show collection-history
		show.CollectionHistoryCommand(cli, basePath),
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

		show.ConfigEtcdCommand(cli, basePath),

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
func RepairCommand(cli clientv3.KV, basePath string) *cobra.Command {
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

// SetCommand, returns etcd set commands.
func SetCommand(cli clientv3.KV, instanceName string, metaPath string) *cobra.Command {
	setCmd := &cobra.Command{
		Use: "set",
	}

	setCmd.AddCommand(
		// by-dev/config not by-dev/meta/config
		set.EtcdConfigCommand(cli, instanceName),
	)

	return setCmd
}

// RemoveCommand returns etcd remove commands.
// WARNING this command shall be used with EXTRA CARE!
func RemoveCommand(cli clientv3.KV, basePath string) *cobra.Command {
	removeCmd := &cobra.Command{
		Use: "remove",
	}

	removeCmd.AddCommand(
		// remove segment
		remove.SegmentCommand(cli, basePath),
		// remove channel
		remove.ChannelCommand(cli, basePath),
		// remove binlog file
		remove.BinlogCommand(cli, basePath),
		// remove collection-drop
		remove.CollectionDropCommand(cli, basePath),
	)

	return removeCmd
}

// RawCommands provides raw "get" command to list kv in etcd
func RawCommands(cli clientv3.KV) []*cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "equivalent to etcd get(withPrefix) command to fetch raw kv values from backup file",
		Run: func(cmd *cobra.Command, args []string) {
			for _, arg := range args {
				fmt.Println("list with", arg)
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
