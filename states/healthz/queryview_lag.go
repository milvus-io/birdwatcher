package healthz

import (
	"context"
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/birdwatcher/states/mgrpc"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type QueryViewLag struct {
	checkItemBase
}

func newQueryViewLag() *QueryViewLag {
	return &QueryViewLag{
		checkItemBase: checkItemBase{
			name: "QUERYVIEW_LAG",
			description: `Checks current whether query view lag is too large.
The check item get all queryview from only items and list all segments from meta.
The problematic queryview contains segments that are absent from meta list, which
are garbage collected.`,
		},
	}
}

func (*QueryViewLag) Check(ctx context.Context, client metakv.MetaKV, basePath string) ([]*HealthzCheckReport, error) {
	segments, err := common.ListSegments(ctx, client, basePath)
	if err != nil {
		return nil, err
	}
	validIDs := lo.SliceToMap(segments, func(segment *models.Segment) (int64, struct{}) { return segment.ID, struct{}{} })

	sessionClients, err := mgrpc.ConnectQueryNodes(ctx, client, basePath, 0)
	if err != nil {
		return nil, err
	}

	var results []*HealthzCheckReport

	for _, sesscli := range sessionClients {
		clientv2 := sesscli.Client
		resp, err := clientv2.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{
			Base: &commonpb.MsgBase{
				SourceID: -1,
				TargetID: sesscli.Session.ServerID,
			},
		})
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		for _, segment := range resp.GetSegments() {
			if _, ok := validIDs[segment.GetID()]; !ok {
				results = append(results, &HealthzCheckReport{
					Msg: fmt.Sprintf("Sealed segment %d still loaded while meta gc-ed", segment.GetID()),
					Extra: map[string]any{
						"segment_id":    segment.GetID(),
						"segment_state": "sealed",
					},
				})
			}
		}

		for _, lv := range resp.GetLeaderViews() {
			growings := lo.Uniq(lo.Union(lv.GetGrowingSegmentIDs(), lo.Keys(lv.GetGrowingSegments())))
			for _, segmentID := range growings {
				if _, ok := validIDs[segmentID]; !ok {
					results = append(results, &HealthzCheckReport{
						Msg: fmt.Sprintf("Sealed segment %d still loaded while meta gc-ed", segmentID),
						Extra: map[string]any{
							"segment_id":    segmentID,
							"segment_state": "growing",
						},
					})
				}
			}
		}
	}
	return results, nil
}
