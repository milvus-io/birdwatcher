package show

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ConfigEtcdCommand return show config-etcd command.
func ConfigEtcdCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config-etcd",
		Short: "list configuations set by etcd source",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			keys, values, err := common.ListEtcdConfigs(ctx, cli, basePath)
			if err != nil {
				fmt.Println("failed to list configurations from etcd", err.Error())
				return
			}

			for i, key := range keys {
				fmt.Printf("Key: %s, Value: %s\n", key, values[i])
			}
		},
	}

	return cmd
}
