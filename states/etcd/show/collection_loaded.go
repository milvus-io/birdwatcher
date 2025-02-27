package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

const (
	ReplicaMetaPrefix = "queryCoord-ReplicaMeta"
)

type CollectionLoadedParam struct {
	framework.ParamBase `use:"show collection-loaded" desc:"display information of loaded collection from querycoord" alias:"collection-load"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to check"`
}

// CollectionLoadedCommand return show collection-loaded command.
func (c *ComponentShow) CollectionLoadedCommand(ctx context.Context, p *CollectionLoadedParam) (*CollectionsLoaded, error) {
	var total int
	infos, err := common.ListCollectionLoadedInfo(ctx, c.client, c.metaPath, func(info *models.CollectionLoaded) bool {
		total++
		return p.CollectionID == 0 || p.CollectionID == info.GetProto().CollectionID
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list collection load info")
	}
	if p.CollectionID > 0 {
		infos = lo.Filter(infos, func(info *models.CollectionLoaded, _ int) bool {
			return info.GetProto().CollectionID == p.CollectionID
		})
	}

	return framework.NewListResult[CollectionsLoaded](infos), nil
}

type CollectionsLoaded struct {
	framework.ListResultSet[*models.CollectionLoaded]
}

func (rs *CollectionsLoaded) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, info := range rs.Data {
			rs.printCollectionLoaded(sb, info)
		}
		fmt.Fprintf(sb, "--- Collections Loaded: %d\n", len(rs.Data))
		return sb.String()
	default:
	}
	return ""
}

func (rs *CollectionsLoaded) printCollectionLoaded(sb *strings.Builder, cl *models.CollectionLoaded) {
	info := cl.GetProto()
	fmt.Fprintf(sb, "CollectionID: %d\n", info.CollectionID)
	fmt.Fprintf(sb, "ReplicaNumber: %d", info.ReplicaNumber)
	fmt.Fprintf(sb, "\tLoadStatus: %s\n", info.Status.String())
}
