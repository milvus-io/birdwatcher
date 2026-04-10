package show

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/errors"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type ReplicaParam struct {
	framework.DataSetParam `use:"show replica" desc:"list current replica information from QueryCoord" alias:"replicas"`
	CollectionID           int64 `name:"collection" default:"0" desc:"collection id to filter with"`
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

	sessions, err := common.ListSessions(ctx, c.client, c.metaPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list sessions")
	}

	rs := framework.NewListResult[Replicas](replicas)
	rs.collections = lo.SliceToMap(collections, func(c *models.Collection) (int64, *models.Collection) { return c.GetProto().GetID(), c })
	rs.sessionMap = lo.SliceToMap(sessions, func(s *models.Session) (int64, *models.Session) { return s.ServerID, s })
	return framework.NewPresetResultSet(rs, framework.NameFormat(p.Format)), nil
}

type Replicas struct {
	framework.ListResultSet[*models.Replica]
	collections map[int64]*models.Collection
	sessionMap  map[int64]*models.Session
}

func (rs *Replicas) TableHeaders() table.Row {
	return table.Row{"ReplicaID", "CollectionID", "ResourceGroup", "Nodes"}
}

func (rs *Replicas) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(rs.Data))
	for _, r := range rs.Data {
		replica := r.GetProto()
		rows = append(rows, table.Row{
			replica.ID, replica.CollectionID,
			replica.ResourceGroup, fmt.Sprintf("%v", replica.Nodes),
		})
	}
	return rows
}

func (rs *Replicas) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		tw := tabwriter.NewWriter(sb, 0, 0, 2, ' ', 0)
		groups := lo.GroupBy(rs.Data, func(replica *models.Replica) int64 { return replica.GetProto().CollectionID })
		for collectionID, replicas := range groups {
			collName := ""
			if coll, ok := rs.collections[collectionID]; ok {
				collName = coll.GetProto().Schema.Name
			}
			fmt.Fprintf(tw, "CollectionID: %d\tCollectionName: %s\n", collectionID, collName)
			for _, replica := range replicas {
				rs.printReplica(tw, replica)
			}
			fmt.Fprintln(tw, "================================================================================")
			fmt.Fprintln(tw)
		}
		tw.Flush()
		return sb.String()
	case framework.FormatJSON:
		return rs.printAsJSON()
	default:
	}
	return ""
}

type nodeInfoJSON struct {
	NodeID   int64  `json:"node_id"`
	HostName string `json:"hostname"`
}

func (rs *Replicas) printAsJSON() string {
	type ShardReplicaJSON struct {
		Shard string         `json:"shard"`
		Nodes []nodeInfoJSON `json:"nodes"`
	}

	type ReplicaJSON struct {
		ReplicaID        int64              `json:"replica_id"`
		CollectionID     int64              `json:"collection_id"`
		ResourceGroup    string             `json:"resource_group"`
		RwNodes          []nodeInfoJSON     `json:"rw_nodes"`
		RoNodes          []nodeInfoJSON     `json:"ro_nodes,omitempty"`
		RwStreamingNodes []nodeInfoJSON     `json:"rw_streaming_nodes,omitempty"`
		RoStreamingNodes []nodeInfoJSON     `json:"ro_streaming_nodes,omitempty"`
		ShardReplicas    []ShardReplicaJSON `json:"shard_replicas,omitempty"`
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
					Nodes: rs.toNodeInfoJSONs(shardReplica.RwNodes),
				})
			}
			replicaJSONs = append(replicaJSONs, ReplicaJSON{
				ReplicaID:        replica.ID,
				CollectionID:     replica.CollectionID,
				ResourceGroup:    replica.ResourceGroup,
				RwNodes:          rs.toNodeInfoJSONs(replica.Nodes),
				RoNodes:          rs.toNodeInfoJSONs(replica.RoNodes),
				RwStreamingNodes: rs.toNodeInfoJSONs(replica.RwSqNodes),
				RoStreamingNodes: rs.toNodeInfoJSONs(replica.RoSqNodes),
				ShardReplicas:    shardReplicas,
			})
		}

		output.Collections = append(output.Collections, CollectionReplicasJSON{
			CollectionID:   collectionID,
			CollectionName: collName,
			Replicas:       replicaJSONs,
		})
	}

	return framework.MarshalJSON(output)
}

func (rs *Replicas) toNodeInfoJSONs(nodeIDs []int64) []nodeInfoJSON {
	if len(nodeIDs) == 0 {
		return nil
	}
	result := make([]nodeInfoJSON, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		result = append(result, nodeInfoJSON{NodeID: id, HostName: rs.getHostName(id)})
	}
	return result
}

func (rs *Replicas) getHostName(nodeID int64) string {
	if sess, ok := rs.sessionMap[nodeID]; ok {
		return sess.HostName
	}
	return "NotFound"
}

func (rs *Replicas) printReplica(w *tabwriter.Writer, r *models.Replica) {
	replica := r.GetProto()
	fmt.Fprintln(w, "================================================================================")
	fmt.Fprintf(w, "ReplicaID: %d\n", replica.ID)
	fmt.Fprintf(w, "ResourceGroup: %s\n", replica.ResourceGroup)
	rs.printNodeList(w, "RW Query Nodes", replica.Nodes)
	rs.printNodeList(w, "RO Query Nodes", replica.RoNodes)
	rs.printNodeList(w, "RW Streaming Nodes", replica.RwSqNodes)
	rs.printNodeList(w, "RO Streaming Nodes", replica.RoSqNodes)
	for shard, shardReplica := range replica.ChannelNodeInfos {
		fmt.Fprintf(w, "-- Shard (%s) Nodes:\n", shard)
		for _, id := range shardReplica.RwNodes {
			fmt.Fprintf(w, "    - %d\t%s\n", id, rs.getHostName(id))
		}
	}
}

func (rs *Replicas) printNodeList(w *tabwriter.Writer, label string, nodeIDs []int64) {
	if len(nodeIDs) == 0 {
		return
	}
	sorted := append([]int64{}, nodeIDs...)
	slices.Sort(sorted)
	fmt.Fprintf(w, "%s (%d):\n", label, len(sorted))
	for _, id := range sorted {
		fmt.Fprintf(w, "  - %d\t%s\n", id, rs.getHostName(id))
	}
}
