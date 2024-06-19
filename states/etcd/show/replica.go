package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type ReplicaParam struct {
	framework.ParamBase `use:"show replica" desc:"list current replica information from QueryCoord" alias:"replicas"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
}

// ReplicaCommand returns command for show querycoord replicas.
func (c *ComponentShow) ReplicaCommand(ctx context.Context, p *ReplicaParam) (*Replicas, error) {
	replicas, err := common.ListReplica(ctx, c.client, c.basePath, p.CollectionID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list replica info")
	}

	return framework.NewListResult[Replicas](replicas), nil
}

type Replicas struct {
	framework.ListResultSet[*models.Replica]
}

func (rs *Replicas) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, replica := range rs.Data {
			rs.printReplica(sb, replica)
		}
		return sb.String()
	default:
	}
	return ""
}

func (rs *Replicas) printReplica(sb *strings.Builder, replica *models.Replica) {
	fmt.Fprintln(sb, "================================================================================")
	fmt.Fprintf(sb, "ReplicaID: %d CollectionID: %d version:%s\n", replica.ID, replica.CollectionID, replica.Version)
	fmt.Fprintf(sb, "All Nodes:%v\n", replica.NodeIDs)
	for _, shardReplica := range replica.ShardReplicas {
		fmt.Fprintf(sb, "-- Shard Replica: Leader ID:%d(%s) Nodes:%v\n", shardReplica.LeaderID, shardReplica.LeaderAddr, shardReplica.NodeIDs)
	}
}
