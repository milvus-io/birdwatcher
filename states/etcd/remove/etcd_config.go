package remove

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// EtcdConfigCommand returns set etcd-config command.
func EtcdConfigCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config-etcd",
		Short: "remove configuations",
		Run: func(cmd *cobra.Command, args []string) {
			key, err := cmd.Flags().GetString("key")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if key == "" {
				fmt.Println("key & value cannot be empty")
				return
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err = common.RemoveEtcdConfig(ctx, cli, basePath, key)
			if err != nil {
				fmt.Println("failed to remove etcd config item,", err.Error())
				return
			}

			fmt.Println("etcd config remove.")
		},
	}

	cmd.Flags().String("key", "", "etcd config key")
	return cmd
}
