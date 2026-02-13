package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type PartitionLoadedParam struct {
	framework.DataSetParam `use:"show partition-loaded" desc:"display the information of loaded partition(s) from querycoord meta"`
	CollectionID           int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	PartitionID            int64 `name:"partition" default:"0" desc:"partition id to filter with"`
}

func (c *ComponentShow) PartitionLoadedCommand(ctx context.Context, p *PartitionLoadedParam) (*framework.PresetResultSet, error) {
	partitions, err := common.ListPartitionLoadedInfo(ctx, c.client, c.metaPath, func(info *models.PartitionLoaded) bool {
		pl := info.GetProto()
		return (p.CollectionID == 0 || p.CollectionID == pl.CollectionID) &&
			(p.PartitionID == 0 || p.PartitionID == pl.PartitionID)
	})
	if err != nil {
		return nil, err
	}
	return framework.NewPresetResultSet(framework.NewListResult[PartitionsLoaded](partitions), framework.NameFormat(p.Format)), nil
}

type PartitionsLoaded struct {
	framework.ListResultSet[*models.PartitionLoaded]
}

func (rs *PartitionsLoaded) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, info := range rs.Data {
			rs.printPartitionLoaded(sb, info)
		}
		fmt.Fprintf(sb, "--- Partitions Loaded: %d\n", len(rs.Data))
		return sb.String()
	case framework.FormatJSON:
		return rs.printAsJSON()
	}
	return ""
}

func (rs *PartitionsLoaded) printAsJSON() string {
	type PartitionLoadedJSON struct {
		CollectionID  int64  `json:"collection_id"`
		PartitionID   int64  `json:"partition_id"`
		ReplicaNumber int32  `json:"replica_number"`
		Status        string `json:"status"`
	}

	type OutputJSON struct {
		Partitions []PartitionLoadedJSON `json:"partitions"`
		Total      int                   `json:"total"`
	}

	output := OutputJSON{
		Partitions: make([]PartitionLoadedJSON, 0, len(rs.Data)),
		Total:      len(rs.Data),
	}

	for _, m := range rs.Data {
		info := m.GetProto()
		output.Partitions = append(output.Partitions, PartitionLoadedJSON{
			CollectionID:  info.CollectionID,
			PartitionID:   info.PartitionID,
			ReplicaNumber: info.ReplicaNumber,
			Status:        info.Status.String(),
		})
	}

	return framework.MarshalJSON(output)
}

func (rs *PartitionsLoaded) printPartitionLoaded(sb *strings.Builder, m *models.PartitionLoaded) {
	info := m.GetProto()
	fmt.Fprintf(sb, "CollectionID: %d\tPartitionID: %d\n", info.CollectionID, info.PartitionID)
	fmt.Fprintf(sb, "ReplicaNumber: %d", info.ReplicaNumber)
	fmt.Fprintf(sb, "\tLoadStatus: %s\n", info.Status.String())
}
