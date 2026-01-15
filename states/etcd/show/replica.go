package show

import (
	"context"
	"encoding/json"
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
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	Format              string `name:"format" default:"" desc:"output format (default, json)"`
}

// ReplicaCommand returns command for show querycoord replicas.
func (c *ComponentShow) ReplicaCommand(ctx context.Context, p *ReplicaParam) (*framework.PresetResultSet, error) {
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
	return framework.NewPresetResultSet(rs, framework.NameFormat(p.Format)), nil
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
	case framework.FormatJSON:
		return rs.printAsJSON()
	default:
	}
	return ""
}

func (rs *Replicas) printAsJSON() string {
	type ShardReplicaJSON struct {
		Shard string  `json:"shard"`
		Nodes []int64 `json:"nodes"`
	}

	type ReplicaJSON struct {
		ReplicaID     int64              `json:"replica_id"`
		CollectionID  int64              `json:"collection_id"`
		ResourceGroup string             `json:"resource_group"`
		Nodes         []int64            `json:"nodes"`
		ShardReplicas []ShardReplicaJSON `json:"shard_replicas,omitempty"`
	}

	type CollectionReplicasJSON struct {
		CollectionID   int64         `json:"collection_id"`
		CollectionName string        `json:"collection_name"`
		Replicas       []ReplicaJSON `json:"replicas"`
	}

	type OutputJSON struct {
		Collections []CollectionReplicasJSON `json:"collections"`
		Total       int                      `json:"total"`
	}

	groups := lo.GroupBy(rs.Data, func(replica *models.Replica) int64 { return replica.GetProto().CollectionID })
	output := OutputJSON{
		Collections: make([]CollectionReplicasJSON, 0, len(groups)),
		Total:       len(rs.Data),
	}

	for collectionID, replicas := range groups {
		collName := ""
		if coll, ok := rs.collections[collectionID]; ok {
			collName = coll.GetProto().Schema.Name
		}

		replicaJSONs := make([]ReplicaJSON, 0, len(replicas))
		for _, r := range replicas {
			replica := r.GetProto()
			shardReplicas := make([]ShardReplicaJSON, 0, len(replica.ChannelNodeInfos))
			for shard, shardReplica := range replica.ChannelNodeInfos {
				shardReplicas = append(shardReplicas, ShardReplicaJSON{
					Shard: shard,
					Nodes: shardReplica.RwNodes,
				})
			}
			replicaJSONs = append(replicaJSONs, ReplicaJSON{
				ReplicaID:     replica.ID,
				CollectionID:  replica.CollectionID,
				ResourceGroup: replica.ResourceGroup,
				Nodes:         replica.Nodes,
				ShardReplicas: shardReplicas,
			})
		}

		output.Collections = append(output.Collections, CollectionReplicasJSON{
			CollectionID:   collectionID,
			CollectionName: collName,
			Replicas:       replicaJSONs,
		})
	}

	bs, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(bs)
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
