package states

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

const (
	scoreBasedBalancePolicy               = "score"
	collectionLabel                       = "collection"
	policyLabel                           = "policy"
	globalRowCountFactorLabel             = "global_factor"
	UnbalanceTolerationFactorLabel        = "unbalance_toleration"
	ReverseUnbalanceTolerationFactorLabel = "reverse_toleration"
)

func ExplainBalanceCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	policies := make(map[string]segmentDistExplainFunc, 0)
	policies[scoreBasedBalancePolicy] = scoreBasedBalanceExplain
	cmd := &cobra.Command{
		Use:   "explain-balance",
		Short: "explain segments and channels current balance status",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// 0. set up collection, policy, servers and params
			collectionID, err := cmd.Flags().GetInt64(collectionLabel)
			if err != nil {
				collectionID = 0
			}
			policyName, err := cmd.Flags().GetString(policyLabel)
			if err != nil {
				policyName = scoreBasedBalancePolicy
			}

			// 1. set up segment distribution view, replicas and segmentInfos
			sessions, err := common.ListServers(ctx, cli, basePath, queryNode)
			if err != nil {
				return err
			}
			distResponses := getAllQueryNodeDistributions(sessions)
			distView := buildUpNodeSegmentsView(distResponses)
			replicas, err := common.ListReplicas(context.Background(), cli, basePath, func(r *models.Replica) bool {
				return collectionID == 0 || collectionID == r.GetProto().GetCollectionID()
			})
			if err != nil {
				fmt.Println("failed to list replica, cannot do balance explain", err.Error())
				return err
			}
			if len(replicas) == 0 {
				fmt.Printf("no replicas available for collection %d, cannot explain balance \n", collectionID)
				return nil
			}
			segmentsInfos, _ := common.ListSegments(context.Background(), cli, basePath)

			// 2. explain balance
			explainPolicy := policies[policyName]
			globalFactor, err := cmd.Flags().GetFloat64(globalRowCountFactorLabel)
			if err != nil {
				globalFactor = 0.1
			}
			unbalanceTolerationFactor, err := cmd.Flags().GetFloat64(UnbalanceTolerationFactorLabel)
			if err != nil {
				unbalanceTolerationFactor = 0.05
			}
			reverseTolerationFactor, err := cmd.Flags().GetFloat64(ReverseUnbalanceTolerationFactorLabel)
			if err != nil {
				reverseTolerationFactor = 1.3
			}
			reports := explainPolicy(distView, replicas, segmentsInfos,
				&ScoreBalanceParam{
					globalFactor, unbalanceTolerationFactor,
					reverseTolerationFactor,
				})
			fmt.Println("explain balance reports:")
			for _, report := range reports {
				fmt.Print(report)
				fmt.Println("------------------------------------")
			}
			return nil
		},
	}
	cmd.Flags().Int64(collectionLabel, 0, "collection id to filter with")
	cmd.Flags().String(policyLabel, "score", "policy to explain based on")
	cmd.Flags().Float64(globalRowCountFactorLabel, 0.1, "global factor")
	cmd.Flags().Float64(UnbalanceTolerationFactorLabel, 0.05, "unbalance toleration factor")
	cmd.Flags().Float64(ReverseUnbalanceTolerationFactorLabel, 1.3, "reverse_toleration")
	return cmd
}

func getAllQueryNodeDistributions(queryNodes []*models.Session) []*querypb.GetDataDistributionResponse {
	distributions := make([]*querypb.GetDataDistributionResponse, 0)
	for _, queryNode := range queryNodes {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		}

		var conn *grpc.ClientConn
		var err error
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			conn, err = grpc.DialContext(ctx, queryNode.Address, opts...)
		}()

		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", queryNode.ServerName, queryNode.ServerID, err.Error())
			continue
		}
		clientv2 := querypb.NewQueryNodeClient(conn)
		resp, err := clientv2.GetDataDistribution(context.Background(), &querypb.GetDataDistributionRequest{
			Base: &commonpb.MsgBase{
				SourceID: -1,
				TargetID: queryNode.ServerID,
			},
		})
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		distributions = append(distributions, resp)
	}
	return distributions
}

func buildUpNodeSegmentsView(distResps []*querypb.GetDataDistributionResponse) map[int64][]*querypb.SegmentVersionInfo {
	distView := make(map[int64][]*querypb.SegmentVersionInfo, 0)
	for _, distResp := range distResps {
		distView[distResp.GetNodeID()] = make([]*querypb.SegmentVersionInfo, 0)
		distView[distResp.GetNodeID()] = append(distView[distResp.GetNodeID()], distResp.GetSegments()...)
	}
	return distView
}

type segmentDistExplainFunc func(dist map[int64][]*querypb.SegmentVersionInfo, replicas []*models.Replica,
	segmentInfos []*models.Segment, scoreBalanceParam *ScoreBalanceParam) []string

func scoreBasedBalanceExplain(dist map[int64][]*querypb.SegmentVersionInfo, replicas []*models.Replica,
	segmentInfos []*models.Segment, scoreBalanceParam *ScoreBalanceParam,
) []string {
	fmt.Printf("replica count:%d \n", len(replicas))
	segmentInfoMap := make(map[int64]*models.Segment, len(segmentInfos))
	for _, seg := range segmentInfos {
		segmentInfoMap[seg.ID] = seg
	}
	sort.Slice(replicas, func(i, j int) bool {
		return (replicas)[i].GetProto().ID <= (replicas)[j].GetProto().ID
	})
	// generate explanation reports for scoreBasedBalance
	reports := make([]string, 0)
	for _, replica := range replicas {
		if replica != nil {
			if len(replica.GetProto().Nodes) > 0 {
				reports = append(reports, explainReplica(replica, dist, segmentInfoMap, scoreBalanceParam))
			} else {
				fmt.Printf("replica %d has no nodes, skip reporting \n", replica.GetProto().ID)
			}
			fmt.Println("---------------------------------------------------------")
		}
	}
	return reports
}

type SegmentItem struct {
	segmentID int64
	rowNum    int64
}

type NodeItem struct {
	nodeID                 int64
	priority               int64
	nodeCollectionSegments []*SegmentItem
	nodeRowSum             int64
	nodeCollectionRowSum   int64
}

type ScoreBalanceParam struct {
	globalRowCountFactor             float64
	UnbalanceTolerationFactor        float64
	ReverseUnbalanceTolerationFactor float64
}

func explainReplica(replica *models.Replica, dist map[int64][]*querypb.SegmentVersionInfo,
	segmentInfoMap map[int64]*models.Segment, param *ScoreBalanceParam,
) string {
	nodeItems := make([]*NodeItem, 0, len(replica.GetProto().Nodes))
	for _, nodeID := range replica.GetProto().Nodes {
		nodeSegments := dist[nodeID]
		var nodeRowSum int64
		var nodeCollectionRowSum int64
		nodeCollectionSegments := make([]*SegmentItem, 0)
		for _, segment := range nodeSegments {
			if segment == nil {
				fmt.Printf("error, get nil segment inside distribution\n")
				return "Wrong segment dist info"
			}
			detailedSegmentInfo, ok := segmentInfoMap[segment.GetID()]
			if !ok {
				fmt.Printf("error, segment %d existed in distribution but not in segment detailedInfoMap\n", segment.GetID())
				return "Wrong segment dist info"
			}
			nodeRowSum += detailedSegmentInfo.NumOfRows
			if segment.GetCollection() == replica.GetProto().CollectionID {
				nodeCollectionRowSum += detailedSegmentInfo.NumOfRows
				nodeCollectionSegments = append(nodeCollectionSegments,
					&SegmentItem{segmentID: segment.GetID(), rowNum: detailedSegmentInfo.NumOfRows})
			}
		}
		priority := int64(param.globalRowCountFactor*float64(nodeRowSum)) + nodeCollectionRowSum
		nodeItems = append(nodeItems, &NodeItem{
			nodeID, priority,
			nodeCollectionSegments, nodeRowSum, nodeCollectionRowSum,
		})
	}
	sort.Slice(nodeItems, func(i, j int) bool {
		return nodeItems[i].priority <= nodeItems[j].priority
	})
	report := fmt.Sprintf(
		"report_collectionID: %d, replicaID: %d, globalRowCountFactor: %f, UnbalanceTolerationFactor: %f ReverseUnbalanceTolerationFactor: %f \n",
		replica.GetProto().CollectionID, replica.GetProto().ID, param.globalRowCountFactor, param.UnbalanceTolerationFactor, param.ReverseUnbalanceTolerationFactor)
	report += fmt.Sprintln("node priorities in asc order:")
	for _, item := range nodeItems {
		report += fmt.Sprintf("[node: %d, priority: %d, node_row_sum: %d, node_collection_row_sum: %d]\n",
			item.nodeID, item.priority, item.nodeRowSum, item.nodeCollectionRowSum)
	}
	// calculate unbalance rate
	toNode, fromNode := nodeItems[0], nodeItems[len(nodeItems)-1]
	unbalanceDiff := fromNode.priority - toNode.priority
	continueBalance := false
	if float64(unbalanceDiff) < float64(toNode.priority)*param.UnbalanceTolerationFactor {
		report += fmt.Sprintf("unbalance diff is only %d, less than toNode priority: %d * %f, stop balancing this replica\n",
			unbalanceDiff, toNode.priority, param.UnbalanceTolerationFactor)
	} else {
		continueBalance = true
	}
	if len(fromNode.nodeCollectionSegments) == 0 {
		return fmt.Sprintf("exception! fromNode %d has no segments, just return\n", fromNode.nodeID) + report
	}
	if continueBalance {
		sort.Slice(fromNode.nodeCollectionSegments, func(i, j int) bool {
			return fromNode.nodeCollectionSegments[i].rowNum <= fromNode.nodeCollectionSegments[j].rowNum
		})
		movedSegment := fromNode.nodeCollectionSegments[0]
		nextFromPriority := int64(float64(fromNode.nodeRowSum-movedSegment.rowNum)*param.globalRowCountFactor) +
			(fromNode.nodeCollectionRowSum - movedSegment.rowNum)
		nextToPriority := int64(float64(toNode.nodeRowSum+movedSegment.rowNum)*param.globalRowCountFactor) +
			(toNode.nodeCollectionRowSum + movedSegment.rowNum)
		movedRowNum := segmentInfoMap[movedSegment.segmentID].NumOfRows
		if nextFromPriority >= nextToPriority {
			report += fmt.Sprintf("should move segment: %d moved_row_num: %d from_node: %d to to_node: %d, next_from_priority: %d, next_to_priority: %d \n",
				movedSegment.segmentID, movedRowNum, fromNode.nodeID, toNode.nodeID, nextFromPriority, nextToPriority)
		} else {
			nextUnbalance := nextToPriority - nextFromPriority
			if float64(nextUnbalance)*param.ReverseUnbalanceTolerationFactor > float64(unbalanceDiff) {
				report += fmt.Sprintf("moved_segment: %d moved_row_num: %d from_node: %d to to_node: %d will generate much bigger unbalance \n",
					movedSegment.segmentID, movedRowNum, fromNode.nodeID, toNode.nodeID)
				report += fmt.Sprintf("next_from_priority: %d, next_to_priority: %d, old_unbalance: %d, next_unbalance: %d \n",
					nextFromPriority, nextToPriority, unbalanceDiff, nextUnbalance)
			} else {
				report += fmt.Sprintf("moved_segment: %d moved_row_num: %d from_node: %d to to_node: %d will reverse unbalance but become less \n",
					movedSegment.segmentID, movedRowNum, fromNode.nodeID, toNode.nodeID)
				report += fmt.Sprintf("next_from_priority: %d, next_to_priority: %d, old_unbalance: %d, next_unbalance: %d \n",
					nextFromPriority, nextToPriority, unbalanceDiff, nextUnbalance)
			}
		}
	}
	return report
}
