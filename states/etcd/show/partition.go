package show

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type PartitionParam struct {
	framework.ParamBase `use:"show partition" desc:"list partitions of provided collection"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to list"`
}

// PartitionCommand returns command to list partition info for provided collection.
func (c *ComponentShow) PartitionCommand(ctx context.Context, p *PartitionParam) (*Partitions, error) {
	if p.CollectionID == 0 {
		return nil, errors.New("collection id not provided")
	}

	partitions, err := common.ListCollectionPartitions(ctx, c.client, c.metaPath, p.CollectionID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list partition info")
	}

	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partition found for collection %d", p.CollectionID)
	}

	return framework.NewListResult[Partitions](partitions), nil
}

type Partitions struct {
	framework.ListResultSet[*models.Partition]
}

func (rs *Partitions) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, info := range rs.Data {
			partition := info.GetProto()
			fmt.Fprintf(sb, "Parition ID: %d\tName: %s\tState: %s\n", partition.GetPartitionID(), partition.GetPartitionName(), partition.State.String())
		}
		fmt.Fprintf(sb, "--- Total Partition(s): %d\n", len(rs.Data))
		return sb.String()
	case framework.FormatJSON:
		return rs.printAsJSON()
	default:
	}
	return ""
}

func (rs *Partitions) printAsJSON() string {
	type PartitionJSON struct {
		PartitionID   int64  `json:"partition_id"`
		PartitionName string `json:"partition_name"`
		State         string `json:"state"`
	}

	type OutputJSON struct {
		Partitions []PartitionJSON `json:"partitions"`
		Total      int             `json:"total"`
	}

	output := OutputJSON{
		Partitions: make([]PartitionJSON, 0, len(rs.Data)),
		Total:      len(rs.Data),
	}

	for _, info := range rs.Data {
		partition := info.GetProto()
		output.Partitions = append(output.Partitions, PartitionJSON{
			PartitionID:   partition.GetPartitionID(),
			PartitionName: partition.GetPartitionName(),
			State:         partition.State.String(),
		})
	}

	bs, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(bs)
}
