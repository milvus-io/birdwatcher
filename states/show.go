package states

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// getEtcdShowCmd returns sub command for instanceState.
// show [subCommand] [options...]
// sub command [collection|session|segment]
func getEtcdShowCmd(cli *clientv3.Client, basePath string) *cobra.Command {
	showCmd := &cobra.Command{
		Use: "show",
	}

	showCmd.AddCommand(
		getEtcdShowCollection(cli, basePath),
		getEtcdShowSession(cli, basePath),
		getEtcdShowSegments(cli, basePath),
		getLoadedSegmentsCmd(cli, basePath),
		getEtcdShowReplica(cli, basePath),
		getCheckpointCmd(cli, basePath),
		getQueryCoordTaskCmd(cli, basePath),
		getQueryCoordClusterNodeInfo(cli, basePath),
		//getEtcdShowIndex(cli, basePath),
		getEtcdShowSegmentIndexCmd(cli, basePath),
		getQueryCoordChannelInfoCmd(cli, basePath),
	)
	return showCmd
}

// getEtcdRawCmd provides raw "get" command to list kv in etcd
func getEtcdRawCmd(cli *clientv3.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use: "get",
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

	return cmd
}
