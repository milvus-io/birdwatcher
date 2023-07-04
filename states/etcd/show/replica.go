package show

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type ReplicaParam struct {
	framework.ParamBase `use:"show replica" desc:"list current replica information from QueryCoord" alias:"replicas"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
}

// ReplicaCommand returns command for show querycoord replicas.
func (c *ComponentShow) ReplicaCommand(ctx context.Context, p *ReplicaParam) {
	replicas, err := common.ListReplica(ctx, c.client, c.basePath, p.CollectionID)
	if err != nil {
		fmt.Println("failed to list replicas", err.Error())
		return
	}

	for _, replica := range replicas {
		printReplica(replica)
	}
}

func printReplica(replica *models.Replica) {
	fmt.Println("================================================================================")
	fmt.Printf("ReplicaID: %d CollectionID: %d version:%s\n", replica.ID, replica.CollectionID, replica.Version)
	fmt.Printf("All Nodes:%v\n", replica.NodeIDs)
	for _, shardReplica := range replica.ShardReplicas {
		fmt.Printf("-- Shard Replica: Leader ID:%d(%s) Nodes:%v\n", shardReplica.LeaderID, shardReplica.LeaderAddr, shardReplica.NodeIDs)
	}
}
