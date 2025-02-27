package show

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

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
	var collections []*models.Collection
	var err error
	if p.CollectionID > 0 {
		collection, err := common.GetCollectionByIDVersion(ctx, c.client, c.metaPath, p.CollectionID)
		if err != nil {
			return nil, err
		}
		collections = []*models.Collection{collection}
	} else {
		collections, err = common.ListCollections(ctx, c.client, c.metaPath, func(c *models.Collection) bool {
			return p.CollectionID == 0 || p.CollectionID == c.GetProto().ID
		})
		if err != nil {
			return nil, err
		}
	}

	replicas, err := common.ListReplicas(ctx, c.client, c.metaPath, func(r *models.Replica) bool {
		return p.CollectionID == 0 || p.CollectionID == r.GetProto().GetCollectionID()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list replica info")
	}

	rs := framework.NewListResult[Replicas](replicas)
	rs.collections = lo.SliceToMap(collections, func(c *models.Collection) (int64, *models.Collection) { return c.GetProto().GetID(), c })
	return rs, nil
}

type Replicas struct {
	framework.ListResultSet[*models.Replica]
	collections map[int64]*models.Collection
}

func (rs *Replicas) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		groups := lo.GroupBy(rs.Data, func(replica *models.Replica) int64 { return replica.GetProto().CollectionID })
		for collectionID, replicas := range groups {
			fmt.Fprintf(sb, "CollectionID: %d\t CollectionName:%s\n", collectionID, rs.collections[collectionID].GetProto().Schema.Name)
			for _, replica := range replicas {
				rs.printReplica(sb, replica)
			}
			fmt.Fprintln(sb, "================================================================================")
			fmt.Fprintln(sb)
		}
		return sb.String()
	default:
	}
	return ""
}

func (rs *Replicas) printReplica(sb *strings.Builder, r *models.Replica) {
	replica := r.GetProto()
	fmt.Fprintln(sb, "================================================================================")
	fmt.Fprintf(sb, "ReplicaID: %d \n", replica.ID)
	fmt.Fprintf(sb, "ResourceGroup: %s\n", replica.ResourceGroup)
	sort.Slice(replica.Nodes, func(i, j int) bool { return replica.Nodes[i] < replica.Nodes[j] })
	fmt.Fprintf(sb, "All Nodes:%v\n", replica.Nodes)
	for shard, shardReplica := range replica.ChannelNodeInfos {
		fmt.Fprintf(sb, "-- Shard Replica: Shard (%s) Nodes:%v\n", shard, shardReplica.RwNodes)
	}
}
