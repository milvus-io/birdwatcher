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

type PartitionParam struct {
	framework.ParamBase `use:"show partition" desc:"list partitions of provided collection"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to list"`
}

// PartitionCommand returns command to list partition info for provided collection.
func (c *ComponentShow) PartitionCommand(ctx context.Context, p *PartitionParam) (*Partitions, error) {
	if p.CollectionID == 0 {
		return nil, errors.New("collection id not provided")
	}

	partitions, err := common.ListCollectionPartitions(ctx, c.client, c.basePath, p.CollectionID)
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
		for _, partition := range rs.Data {
			fmt.Fprintf(sb, "Parition ID: %d\tName: %s\tState: %s\n", partition.ID, partition.Name, partition.State.String())
		}
		fmt.Fprintf(sb, "--- Total Partition(s): %d\n", len(rs.Data))
		return sb.String()
	default:
	}
	return ""
}
