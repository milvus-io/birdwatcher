package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

type SegmentLoadedParam struct {
	framework.ParamBase `use:"show segment-loaded" desc:"display segment information from querycoordv1 meta" alias:"segments-loaded"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	SegmentID           int64 `name:"segment" default:"0" desc:"segment id to filter with"`
}

func (c *ComponentShow) SegmentLoadedCommand(ctx context.Context, p *SegmentLoadedParam) (*LoadedSegments, error) {
	if etcdversion.GetVersion() != models.LTEVersion2_1 {
		return nil, errors.New("list segment-loaded from meta only support before 2.1.x, try use `show segment-loaded-grpc` instead")
	}
	segments, err := common.ListLoadedSegments(c.client, c.basePath, func(info *querypb.SegmentInfo) bool {
		return (p.CollectionID == 0 || info.CollectionID == p.CollectionID) &&
			(p.SegmentID == 0 || info.SegmentID == p.SegmentID)
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list loaded segments")
	}

	return framework.NewListResult[LoadedSegments](segments), nil
}

type LoadedSegments struct {
	framework.ListResultSet[querypb.SegmentInfo]
}

func (rs *LoadedSegments) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		builder := &strings.Builder{}
		for _, info := range rs.Data {
			fmt.Fprintf(builder, "Segment ID: %d LegacyNodeID: %d NodeIds: %v,DmlChannel: %s\n", info.SegmentID, info.NodeID, info.NodeIds, info.DmChannel)
			fmt.Fprintf(builder, "%#v\n", info.GetIndexInfos())
		}
		return builder.String()
	default:
	}
	return ""
}
