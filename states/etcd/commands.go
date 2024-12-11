package etcd

import (
	"context"
	"fmt"
	"path"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/states/etcd/download"
	"github.com/milvus-io/birdwatcher/states/etcd/remove"
	"github.com/milvus-io/birdwatcher/states/etcd/repair"
	"github.com/milvus-io/birdwatcher/states/etcd/set"
	"github.com/milvus-io/birdwatcher/states/etcd/show"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ShowCommand returns sub command for instanceState.
// show [subCommand] [options...]
// sub command [collection|session|segment]
func ShowCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	showCmd := &cobra.Command{
		Use: "show",
	}

	showCmd.AddCommand(
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
func RepairCommand(cli kv.MetaKV, basePath string) *cobra.Command {
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
		// repair miss index metric_type
		repair.IndexMetricCommand(cli, basePath),
		repair.DiskAnnIndexParamsCommand(cli, basePath),
		repair.AddIndexParamsCommand(cli, basePath),
		// repair manual compaction
		repair.ManualCompactionCommand(cli, basePath),
		// check querynode collection leak
		repair.CheckQNCollectionLeak(cli, basePath),
	)

	return repairCmd
}

// SetCommand, returns etcd set commands.
func SetCommand(cli kv.MetaKV, instanceName string, metaPath string) *cobra.Command {
	setCmd := &cobra.Command{
		Use: "set",
	}

	setCmd.AddCommand(
		// by-dev/config not by-dev/meta/config
		set.EtcdConfigCommand(cli, instanceName),
		set.FieldAlterCommand(cli, path.Join(instanceName, metaPath)),
	)

	return setCmd
}

// RemoveCommand returns etcd remove commands.
// WARNING this command shall be used with EXTRA CARE!
func RemoveCommand(cli kv.MetaKV, instanceName, basePath string) *cobra.Command {
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
		// remove sgements with collection dropped
		remove.SegmentCollectionDroppedCommand(cli, basePath),
		// remove etcd-config
		remove.EtcdConfigCommand(cli, instanceName),
		// remove collection has been dropped
		remove.CollectionCleanCommand(cli, basePath),
		// remove import job
		remove.ImportJob(cli, basePath),
	)

	return removeCmd
}

// RawCommands provides raw "get" command to list kv in etcd
func RawCommands(cli kv.MetaKV) []*cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "equivalent to etcd get(withPrefix) command to fetch raw kv values from backup file",
		Run: func(cmd *cobra.Command, args []string) {
			withValue, err := cmd.Flags().GetBool("withValue")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			var options []kv.LoadOption
			if !withValue {
				options = append(options, kv.WithKeysOnly())
			}
			for _, arg := range args {
				fmt.Println("list with", arg)
				keys, vals, err := cli.LoadWithPrefix(context.Background(), arg, options...)
				if err != nil {
					fmt.Println(err.Error())
					continue
				}
				if len(keys) != len(vals) {
					fmt.Printf("unmatched kv sizes for %s: len(keys): %d, len(vals): %d.", arg, len(keys), len(vals))
					continue
				}
				for i, key := range keys {
					fmt.Printf("key: %s\n", key)
					if withValue {
						fmt.Printf("Value: %s\n", vals[i])
					}
				}
			}
		},
	}

	cmd.Flags().Bool("withValue", false, "print values")
	return []*cobra.Command{cmd}
}

func DownloadCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "download",
		Short: "download etcd data",
	}
	cmd.AddCommand(
		// download global-distribution
		download.PullGlobalDistributionDetails(cli, basePath),
	)
	return cmd
}
