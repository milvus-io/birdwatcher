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
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
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
		collection, err := common.GetCollectionByIDVersion(ctx, c.client, c.metaPath, etcdversion.GetVersion(), p.CollectionID)
		if err != nil {
			return nil, err
		}
		collections = []*models.Collection{collection}
	} else {
		collections, err = common.ListCollectionsVersion(ctx, c.client, c.metaPath, etcdversion.GetVersion(), func(c *models.Collection) bool {
			return p.CollectionID == 0 || p.CollectionID == c.ID
		})
		if err != nil {
			return nil, err
		}
	}

	replicas, err := common.ListReplica(ctx, c.client, c.metaPath, p.CollectionID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list replica info")
	}

	rs := framework.NewListResult[Replicas](replicas)
	rs.collections = lo.SliceToMap(collections, func(c *models.Collection) (int64, *models.Collection) { return c.ID, c })
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
		groups := lo.GroupBy(rs.Data, func(replica *models.Replica) int64 { return replica.CollectionID })
		for collectionID, replicas := range groups {
			fmt.Fprintf(sb, "CollectionID: %d\t CollectionName:%s\n", collectionID, rs.collections[collectionID].Schema.Name)
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

func (rs *Replicas) printReplica(sb *strings.Builder, replica *models.Replica) {
	fmt.Fprintln(sb, "================================================================================")
	fmt.Fprintf(sb, "ReplicaID: %d version:%s\n", replica.ID, replica.Version)
	fmt.Fprintf(sb, "ResourceGroup: %s\n", replica.ResourceGroup)
	sort.Slice(replica.NodeIDs, func(i, j int) bool { return replica.NodeIDs[i] < replica.NodeIDs[j] })
	fmt.Fprintf(sb, "All Nodes:%v\n", replica.NodeIDs)
	for _, shardReplica := range replica.ShardReplicas {
		fmt.Fprintf(sb, "-- Shard Replica: Leader ID:%d(%s) Nodes:%v\n", shardReplica.LeaderID, shardReplica.LeaderAddr, shardReplica.NodeIDs)
	}
}
