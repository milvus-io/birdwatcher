package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

type PartitionLoadedParam struct {
	framework.ParamBase `use:"show partition-loaded" desc:"display the information of loaded partition(s) from querycoord meta"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	PartitionID         int64 `name:"partition" default:"0" desc:"partition id to filter with"`
}

func (c *ComponentShow) PartitionLoadedCommand(ctx context.Context, p *PartitionLoadedParam) (*PartitionsLoaded, error) {
	partitions, err := common.ListPartitionLoadedInfo(ctx, c.client, c.metaPath, etcdversion.GetVersion(), func(pl *models.PartitionLoaded) bool {
		return (p.CollectionID == 0 || p.CollectionID == pl.CollectionID) &&
			(p.PartitionID == 0 || p.PartitionID == pl.PartitionID)
	})
	if err != nil {
		return nil, err
	}
	return framework.NewListResult[PartitionsLoaded](partitions), nil
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
	default:
	}
	return ""
}

func (rs *PartitionsLoaded) printPartitionLoaded(sb *strings.Builder, info *models.PartitionLoaded) {
	fmt.Fprintf(sb, "CollectionID: %d\tPartitionID: %d\n", info.CollectionID, info.PartitionID)
	fmt.Fprintf(sb, "ReplicaNumber: %d", info.ReplicaNumber)
	switch info.Version {
	case models.LTEVersion2_1:
	case models.GTEVersion2_2:
		fmt.Fprintf(sb, "\tLoadStatus: %s\n", info.Status.String())
	}
}
