package states

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type HealthzCheckParam struct {
	framework.ParamBase `use:"healthz-check" desc:"perform healthz check for connect instance"`
}

type HealthzCheckReports struct {
	framework.ListResultSet[*HealthzCheckReport]
}

func (rs *HealthzCheckReports) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, report := range rs.Data {
			fmt.Fprintln(sb, report.Msg)
		}
		return sb.String()
	case framework.FormatJSON:
		sb := &strings.Builder{}
		for _, report := range rs.Data {
			output := report.Extra
			bs, err := json.Marshal(output)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			sb.Write(bs)
			sb.WriteString("\n")
		}
		return sb.String()
	default:
	}
	return ""
}

type HealthzCheckReport struct {
	Msg   string
	Extra map[string]any
}

func (c *InstanceState) HealthzCheckCommand(ctx context.Context, p *HealthzCheckParam) (*framework.PresetResultSet, error) {

	results, err := c.checkSegmentTarget(ctx)
	if err != nil {
		return nil, err
	}

	return framework.NewPresetResultSet(framework.NewListResult[HealthzCheckReports](results), framework.FormatJSON), nil
}

func (c *InstanceState) checkSegmentTarget(ctx context.Context) ([]*HealthzCheckReport, error) {
	segments, err := common.ListSegmentsVersion(ctx, c.client, c.basePath, etcdversion.GetVersion())
	if err != nil {
		return nil, err
	}
	validIDs := lo.SliceToMap(segments, func(segment *models.Segment) (int64, struct{}) { return segment.ID, struct{}{} })

	sessions, err := common.ListSessions(c.client, c.basePath)
	if err != nil {
		return nil, err
	}

	var results []*HealthzCheckReport

	for _, session := range sessions {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		}

		conn, err := grpc.DialContext(ctx, session.Address, opts...)
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		if session.ServerName == "querynode" {
			clientv2 := querypbv2.NewQueryNodeClient(conn)
			resp, err := clientv2.GetDataDistribution(ctx, &querypbv2.GetDataDistributionRequest{
				Base: &commonpbv2.MsgBase{
					SourceID: -1,
					TargetID: session.ServerID,
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
	}
	return results, nil
}
