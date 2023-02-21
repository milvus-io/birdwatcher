package show

import (
	"fmt"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ReplicaCommand returns command for show querycoord replicas.
func ReplicaCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "replica",
		Short:   "list current replica information from QueryCoord",
		Aliases: []string{"replicas"},
		Run: func(cmd *cobra.Command, args []string) {
			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			replicas, err := common.ListReplica(cli, basePath, collID)
			if err != nil {
				fmt.Println("failed to list replicas", err.Error())
				return
			}

			for _, replica := range replicas {
				printReplica(replica)
			}
		},
	}

	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	return cmd
}

func printReplica(replica *models.Replica) {
	fmt.Println("================================================================================")
	fmt.Printf("ReplicaID: %d CollectionID: %d version:%s\n", replica.ID, replica.CollectionID, replica.Version)
	fmt.Printf("All Nodes:%v\n", replica.NodeIDs)
	for _, shardReplica := range replica.ShardReplicas {
		fmt.Printf("-- Shard Replica: Leader ID:%d(%s) Nodes:%v\n", shardReplica.LeaderID, shardReplica.LeaderAddr, shardReplica.NodeIDs)
	}
}
