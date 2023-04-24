package set

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdConfigCommand returns set etcd-config command.
func EtcdConfigCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config-etcd",
		Short: "list configuations set by etcd source",
		Run: func(cmd *cobra.Command, args []string) {
			key, err := cmd.Flags().GetString("key")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			value, err := cmd.Flags().GetString("value")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if key == "" || value == "" {
				fmt.Println("key & value cannot be empty")
				return
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err = common.SetEtcdConfig(ctx, cli, basePath, key, value)
			if err != nil {
				fmt.Println("failed to set etcd config item,", err.Error())
				return
			}

			fmt.Println("etcd config set.")
		},
	}

	cmd.Flags().String("key", "", "etcd config key")
	cmd.Flags().String("value", "", "etcd config value")
	return cmd
}
