package show

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/milvus-io/birdwatcher/proto/v2.0/milvuspb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ReplicaCommand returns command for show querycoord replicas.
func ReplicaCommand(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "replica",
		Short:   "list current replica information from QueryCoord",
		Aliases: []string{"replicas"},
		RunE: func(cmd *cobra.Command, args []string) error {
			replicas, err := listReplicas(cli, basePath)

			if err != nil {
				return err
			}

			for _, replica := range replicas {
				printReplica(replica)
			}
			return nil
		},
	}

	return cmd
}

func listReplicas(cli *clientv3.Client, basePath string) ([]milvuspb.ReplicaInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	prefix := path.Join(basePath, "queryCoord-ReplicaMeta")

	replicas, _, err := common.ListProtoObjects[milvuspb.ReplicaInfo](ctx, cli, prefix)

	if err != nil {
		return nil, err
	}

	return replicas, nil
}

func printReplica(replica milvuspb.ReplicaInfo) {
	fmt.Println("================================================================================")
	fmt.Printf("ReplicaID: %d CollectionID: %d\n", replica.ReplicaID, replica.CollectionID)
	for _, shardReplica := range replica.ShardReplicas {
		fmt.Printf("Channel %s leader %d\n", shardReplica.DmChannelName, shardReplica.LeaderID)
	}
	fmt.Printf("Nodes:%v\n", replica.NodeIds)
}
