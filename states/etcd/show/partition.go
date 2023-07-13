package show

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type PartitionParam struct {
	framework.ParamBase `use:"show partition" desc:"list partitions of provided collection"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to list"`
}

// PartitionCommand returns command to list partition info for provided collection.
func (c *ComponentShow) PartitionCommand(ctx context.Context, p *PartitionParam) {
	if p.CollectionID == 0 {
		fmt.Println("please provided collection id")
		return
	}

	partitions, err := common.ListCollectionPartitions(ctx, c.client, c.basePath, p.CollectionID)
	if err != nil {
		fmt.Println("failed to list partition info", err.Error())
	}

	if len(partitions) == 0 {
		fmt.Printf("no partition found for collection %d\n", p.CollectionID)
	}

	for _, partition := range partitions {
		fmt.Printf("Parition ID: %d\tName: %s\tState: %s\n", partition.ID, partition.Name, partition.State.String())
	}
}
