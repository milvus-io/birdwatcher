package etcd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/states/etcd/download"
	"github.com/milvus-io/birdwatcher/states/etcd/repair"
	"github.com/milvus-io/birdwatcher/states/etcd/set"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ShowCommand returns sub command for instanceState.
// show [subCommand] [options...]
// sub command [collection|session|segment]
func ShowCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	showCmd := &cobra.Command{
		Use: "show",
	}

	return showCmd
}

// RepairCommand returns etcd repair commands.
func RepairCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	repairCmd := &cobra.Command{
		Use: "repair",
	}

	repairCmd.AddCommand(
		// repair miss index metric_type
		repair.IndexMetricCommand(cli, basePath),
		repair.DiskAnnIndexParamsCommand(cli, basePath),
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
	)

	return setCmd
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
