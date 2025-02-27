package repair

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

func CheckQNCollectionLeak(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check_qn_collection_leak",
		Short: "check whether querynode has collection leak",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("check querynode collection leak\n")
			sessions, err := common.ListSessions(context.Background(), cli, basePath)
			if err != nil {
				fmt.Printf("failed to list sessions")
				return err
			}

			rcClient, err := getRootCoordClient(sessions)
			if err != nil {
				fmt.Printf("failed to get querycoord client")
				return err
			}

			resp1, err := rcClient.ShowCollections(context.Background(), &milvuspb.ShowCollectionsRequest{
				Base: &commonpb.MsgBase{
					SourceID: -1,
					MsgType:  commonpb.MsgType_ShowCollections,
				},
			})
			if err != nil {
				fmt.Printf("failed to get rootcoord collections, err=%s", err.Error())
				return err
			}
			collectionsOnRC := resp1.CollectionIds
			fmt.Printf("rootcoord collections: %v\n", collectionsOnRC)

			qcClient, err := getQueryCoordClient(sessions)
			if err != nil {
				fmt.Printf("failed to get querycoord client")
				return err
			}
			req, _ := ConstructRequestByMetricType("system_info")
			resp, err := qcClient.GetMetrics(context.Background(), req)
			if err != nil {
				fmt.Printf("failed to get querycoord metrics, err=%s", err.Error())
				return err
			}

			queryCoordTopology := &models.QueryCoordTopology{}
			if err := json.Unmarshal([]byte(resp.GetResponse()), queryCoordTopology); err != nil {
				fmt.Printf("failed to unmarshal querycoord metrics, len=%d", len(resp.GetResponse()))
				return err
			}

			for _, qnMetrics := range queryCoordTopology.Cluster.ConnectedNodes {
				collectionsOnQN := qnMetrics.QuotaMetrics.Effect.CollectionIDs
				_, leakCollections := lo.Difference(collectionsOnRC, collectionsOnQN)
				if len(leakCollections) > 0 {
					fmt.Printf("querynode %d has leak collections: %v\n", qnMetrics.ID, leakCollections)
				}
			}

			return nil
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	return cmd
}

func getQueryCoordClient(sessions []*models.Session) (querypb.QueryCoordClient, error) {
	for _, session := range sessions {
		if strings.ToLower(session.ServerName) != "querycoord" {
			continue
		}

		opts := []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithTimeout(2 * time.Second),
		}

		conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		client := querypb.NewQueryCoordClient(conn)
		return client, nil
	}
	return nil, errors.New("querycoord session not found")
}

func getRootCoordClient(sessions []*models.Session) (rootcoordpb.RootCoordClient, error) {
	for _, session := range sessions {
		if strings.ToLower(session.ServerName) != "rootcoord" {
			continue
		}

		opts := []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithTimeout(2 * time.Second),
		}

		conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		client := rootcoordpb.NewRootCoordClient(conn)
		return client, nil
	}
	return nil, errors.New("querycoord session not found")
}

func ConstructRequestByMetricType(metricType string) (*milvuspb.GetMetricsRequest, error) {
	m := make(map[string]interface{})
	m["metric_type"] = metricType
	binary, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to construct request by metric type %s: %s", metricType, err.Error())
	}
	// TODO:: switch metricType to different msgType and return err when metricType is not supported
	return &milvuspb.GetMetricsRequest{
		Request: string(binary),
	}, nil
}
