package states

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/models"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	internalpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/internalpb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/planpb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	schemapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/schemapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

func GetProbeCmd(cli clientv3.KV, basePath string) *cobra.Command {
	probeCmd := &cobra.Command{
		Use:   "probe",
		Short: "probe service state with internal apis",
	}

	probeCmd.AddCommand(
		// probe query
		getProbeQueryCmd(cli, basePath),
		// probe pk
		getProbePKCmd(cli, basePath),
	)

	return probeCmd
}

func getProbeQueryCmd(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "probe query service",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			loaded, err := common.ListCollectionLoadedInfo(ctx, cli, basePath, models.GTEVersion2_2)
			if err != nil {
				fmt.Println("failed to list loaded collection", err.Error())
				return
			}

			if len(loaded) == 0 {
				fmt.Println("no loaded collection")
				return
			}

			sessions, err := common.ListSessions(ctx, cli, basePath)
			if err != nil {
				fmt.Println("failed to list online sessions", err.Error())
				return
			}

			qc, err := getQueryCoordClient(sessions)
			if err != nil {
				fmt.Println("failed to connect querycoord", err.Error())
				return
			}

			qns, err := getQueryNodeClients(sessions)
			if err != nil {
				fmt.Println("failed to connect querynodes", err.Error())
				return
			}
			if len(qns) == 0 {
				fmt.Println("no querynode online")
				return
			}

			for _, collection := range loaded {
				fmt.Println("probing collection", collection.CollectionID)
				req, err := getMockSearchRequest(ctx, cli, basePath, collection)
				if err != nil {
					fmt.Println("failed to generated mock request", err.Error())
					continue
				}

				leaders, err := qc.GetShardLeaders(ctx, &querypbv2.GetShardLeadersRequest{
					Base:         &commonpbv2.MsgBase{},
					CollectionID: collection.CollectionID,
				})
				if err != nil {
					fmt.Println("querycoord get shard leaders error", err.Error())
					continue
				}

				for _, shard := range leaders.GetShards() {
					for _, nodeID := range shard.GetNodeIds() {
						qn, ok := qns[nodeID]
						if !ok {
							fmt.Printf("Shard leader %d not online\n", nodeID)
							continue
						}

						ctx, cancel := context.WithTimeout(ctx, time.Second*5)
						req.DmlChannels = []string{shard.GetChannelName()}
						req.Req.Base.TargetID = nodeID
						resp, err := qn.Search(ctx, req)
						cancel()
						if err != nil {
							fmt.Printf("Shard %s Leader[%d] failed to search with eventually consistency level, err: %s\n", shard.GetChannelName(), nodeID, err.Error())
							continue
						}
						if resp.GetStatus().GetErrorCode() != commonpbv2.ErrorCode_Success {
							fmt.Printf("Shard %s Leader[%d] failed to search,error code: %s reason:%s\n", shard.GetChannelName(), nodeID, resp.GetStatus().GetErrorCode().String(), resp.GetStatus().GetReason())
							continue
						}
						fmt.Printf("Shard %s leader[%d] probe with search success.\n", shard.GetChannelName(), nodeID)
					}
				}
			}
		},
	}

	return cmd
}

func getProbePKCmd(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pk",
		Short: "probe pk in segment",
		Run: func(cmd *cobra.Command, args []string) {
			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			pk, err := cmd.Flags().GetString("pk")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			coll, err := common.GetCollectionByIDVersion(ctx, cli, basePath, etcdversion.GetVersion(), collID)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			pkf, _ := coll.GetPKField()
			var datatype schemapbv2.DataType
			var val *planpb.GenericValue
			switch pkf.DataType {
			case models.DataTypeVarChar:
				datatype = schemapbv2.DataType_VarChar
				val = &planpb.GenericValue{
					Val: &planpb.GenericValue_StringVal{
						StringVal: pk,
					},
				}
			case models.DataTypeInt64:
				datatype = schemapbv2.DataType_Int64
				pkv, err := strconv.ParseInt(pk, 10, 64)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				val = &planpb.GenericValue{
					Val: &planpb.GenericValue_Int64Val{
						Int64Val: pkv,
					},
				}
			}

			var outputFields []int64
			fieldIDName := make(map[int64]string)
			for _, f := range coll.Schema.Fields {
				if f.FieldID >= 100 {
					outputFields = append(outputFields, f.FieldID)
				}
				fieldIDName[f.FieldID] = f.Name
			}
			plan := &planpb.PlanNode{
				Node: &planpb.PlanNode_Predicates{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_TermExpr{
							TermExpr: &planpb.TermExpr{
								ColumnInfo: &planpb.ColumnInfo{
									FieldId:      pkf.FieldID,
									DataType:     datatype,
									IsAutoID:     pkf.AutoID,
									IsPrimaryKey: pkf.IsPrimaryKey,
								},
								Values: []*planpb.GenericValue{
									val,
								},
							},
						},
					},
				},
				OutputFieldIds: outputFields,
			}

			bs, _ := proto.Marshal(plan)

			sessions, err := common.ListSessions(ctx, cli, basePath)
			if err != nil {
				fmt.Println("failed to list online sessions", err.Error())
				return
			}

			qc, err := getQueryCoordClient(sessions)
			if err != nil {
				fmt.Println("failed to connect querycoord", err.Error())
				return
			}

			resp, err := qc.GetShardLeaders(ctx, &querypbv2.GetShardLeadersRequest{
				Base:         &commonpbv2.MsgBase{},
				CollectionID: collID,
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			if resp.GetStatus().GetErrorCode() != commonpbv2.ErrorCode_Success {
				fmt.Println("failed to list shard leader", resp.GetStatus().GetErrorCode())
				return
			}

			qns, err := getQueryNodeClients(sessions)
			if err != nil {
				fmt.Println("failed to connect querynodes", err.Error())
				return
			}
			if len(qns) == 0 {
				fmt.Println("no querynode online")
				return
			}

			for nodeID, qn := range qns {
				resp, err := qn.GetDataDistribution(ctx, &querypbv2.GetDataDistributionRequest{
					Base: &commonpbv2.MsgBase{TargetID: nodeID},
				})
				if err != nil {
					fmt.Println("failed to get data distribution from node", nodeID, err.Error())
					continue
				}
				for _, segInfo := range resp.GetSegments() {
					if segInfo.GetCollection() != collID {
						continue
					}
					result, err := qn.Query(ctx, &querypbv2.QueryRequest{
						SegmentIDs:      []int64{segInfo.ID},
						DmlChannels:     []string{segInfo.GetChannel()},
						FromShardLeader: true, // query single segment
						Scope:           querypbv2.DataScope_Historical,
						Req: &internalpbv2.RetrieveRequest{
							Base:               &commonpbv2.MsgBase{TargetID: nodeID, MsgID: time.Now().Unix()},
							CollectionID:       segInfo.Collection,
							PartitionIDs:       []int64{segInfo.Partition},
							SerializedExprPlan: bs,
							OutputFieldsId:     outputFields,
							Limit:              -1, // unlimited
						},
					})
					if err != nil {
						fmt.Println(err.Error())
						continue
					}
					if result.GetStatus().GetErrorCode() != commonpbv2.ErrorCode_Success {
						fmt.Printf("failed to probe pk on segment %d, reason: %s\n", segInfo.GetID(), result.GetStatus().GetReason())
						continue
					}
					if GetSizeOfIDs(result.GetIds()) == 0 {
						continue
					}
					fmt.Printf("PK %s found on segment %d\n", pk, segInfo.GetID())
					for _, fd := range result.GetFieldsData() {
						fmt.Printf("Field %s, value: %v\n", fieldIDName[fd.GetFieldId()], fd.GetField())
					}
				}
			}
		},
	}

	cmd.Flags().Int64("collection", 0, "collection id for probe")
	cmd.Flags().String("pk", "", "pk value to probe")

	return cmd
}

func GetSizeOfIDs(data *schemapbv2.IDs) int {
	result := 0
	if data.IdField == nil {
		return result
	}

	switch data.GetIdField().(type) {
	case *schemapbv2.IDs_IntId:
		result = len(data.GetIntId().GetData())
	case *schemapbv2.IDs_StrId:
		result = len(data.GetStrId().GetData())
	default:
	}

	return result
}

func getQueryCoordClient(sessions []*models.Session) (querypbv2.QueryCoordClient, error) {
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

		client := querypbv2.NewQueryCoordClient(conn)
		return client, nil
	}
	return nil, errors.New("querycoord session not found")
}

func getQueryNodeClients(sessions []*models.Session) (map[int64]querypbv2.QueryNodeClient, error) {
	result := make(map[int64]querypbv2.QueryNodeClient)

	for _, session := range sessions {
		if strings.ToLower(session.ServerName) != "querynode" {
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

		client := querypbv2.NewQueryNodeClient(conn)
		result[session.ServerID] = client
	}

	return result, nil
}

func getMockSearchRequest(ctx context.Context, cli clientv3.KV, basePath string, collection *models.CollectionLoaded) (*querypbv2.SearchRequest, error) {
	coll, err := common.GetCollectionByIDVersion(ctx, cli, basePath, models.GTEVersion2_2, collection.CollectionID)
	if err != nil {
		return nil, err
	}
	pkField, ok := coll.GetPKField()
	if !ok {
		return nil, errors.New("pk field not found")
	}
	vectorField, ok := coll.GetVectorField()
	if !ok {
		return nil, errors.New("vector field not found")
	}
	dim, err := vectorField.GetDim()
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	indexID := collection.FieldIndexID[vectorField.FieldID]
	fmt.Printf("Found vector field %s(%d) with dim[%d], indexID: %d\n", vectorField.Name, vectorField.FieldID, dim, indexID)

	indexes, _, err := common.ListProtoObjects(ctx, cli, path.Join(basePath, "field-index"), func(index *indexpbv2.FieldIndex) bool {
		return index.GetIndexInfo().GetIndexID() == indexID
	})
	if err != nil {
		return nil, err
	}

	if len(indexes) != 1 {
		fmt.Println("multiple or zero index found, bad meta")
		return nil, err
	}
	vector := genFloatVector(dim)

	req := &internalpbv2.SearchRequest{
		Base: &commonpbv2.MsgBase{
			MsgType: commonpbv2.MsgType_Search,
		},
		CollectionID:       coll.ID,
		PartitionIDs:       []int64{},
		Dsl:                "",
		PlaceholderGroup:   vector2PlaceholderGroupBytes(vector),
		DslType:            commonpbv2.DslType_BoolExprV1,
		GuaranteeTimestamp: 1, // Eventually first
		Nq:                 1,
	}

	index := indexes[0]

	indexType := common.GetKVPair(index.GetIndexInfo().GetIndexParams(), "index_type")

	switch indexType {
	case "HNSW":
		raw := common.GetKVPair(index.GetIndexInfo().GetIndexParams(), "efConstruction")
		efConstruction, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			efConstruction = 360 // in case of auto index
		}
		metricType := common.GetKVPair(index.GetIndexInfo().GetIndexParams(), "metric_type")
		if metricType == "" {
			metricType = common.GetKVPair(index.GetIndexInfo().GetTypeParams(), "metric_type")
			if metricType == "" {
				fmt.Println("no metric_type in IndexParams or TypeParams")
				return nil, fmt.Errorf("no metric_type in IndexParams or TypeParams, bad meta")
			}
			fmt.Println("metric_type is in TypeParams instead of IndexParams")
		}
		topK := rand.Int63n(efConstruction-1) + 1

		/*
			searchParams := map[string]string{
				"anns_field":    vectorField.Name,
				"topk":          fmt.Sprintf("%d", topK),
				"params":        string(genSearchHNSWParamBytes(topK)),
				"metric_type":   metricType,
				"round_decimal": "-1",
			}
			bs, err := json.Marshal(searchParams)
			spStr := string(bs)
			fmt.Println("search params", spStr)*/
		spStr := genSearchHNSWParamBytes(topK)

		req.SerializedExprPlan = getSearchPlan(vectorField.DataType == models.DataTypeBinaryVector, pkField.FieldID, vectorField.FieldID, topK, metricType, string(spStr))

		r := &querypbv2.SearchRequest{
			Req:             req,
			FromShardLeader: false,
			DmlChannels:     []string{},
		}
		return r, nil
	case "DISKANN":
		metricType := common.GetKVPair(index.GetIndexInfo().GetIndexParams(), "metric_type")
		if metricType == "" {
			metricType = common.GetKVPair(index.GetIndexInfo().GetTypeParams(), "metric_type")
			if metricType == "" {
				fmt.Println("no metric_type in IndexParams or TypeParams")
				return nil, fmt.Errorf("no metric_type in IndexParams or TypeParams, bad meta")
			}
			fmt.Println("metric_type is in TypeParams instead of IndexParams")
		}

		topK := int64(10)
		spStr := genSearchDISKANNParamBytes(20)
		req.SerializedExprPlan = getSearchPlan(vectorField.DataType == models.DataTypeBinaryVector, pkField.FieldID, vectorField.FieldID, topK, metricType, string(spStr))
		r := &querypbv2.SearchRequest{
			Req:             req,
			FromShardLeader: false,
			DmlChannels:     []string{},
		}
		return r, nil

	default:
		return nil, fmt.Errorf("probing index type %s not supported yet", indexType)
	}
}

func getSearchPlan(isBinary bool, pkFieldID, vectorFieldID int64, topk int64, metricType string, searchParam string) []byte {
	plan := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				IsBinary:   isBinary,
				Predicates: nil, // empty
				QueryInfo: &planpb.QueryInfo{
					Topk:         topk,
					MetricType:   metricType,
					SearchParams: searchParam,
					RoundDecimal: -1,
				},
				PlaceholderTag: "$0",
				FieldId:        vectorFieldID,
			},
		},
		OutputFieldIds: []int64{pkFieldID},
	}

	bs, _ := proto.Marshal(plan)

	return bs
}

func genSearchHNSWParamBytes(ef int64) []byte {
	m := make(map[string]any)
	m["ef"] = ef
	bs, _ := json.Marshal(m)
	return bs
}

func genSearchDISKANNParamBytes(searchList int) []byte {
	m := make(map[string]any)
	m["search_list"] = searchList
	bs, _ := json.Marshal(m)
	return bs
}

func genFloatVector(dim int64) FloatVector {
	result := make([]float32, 0, dim)

	for i := int64(0); i < dim; i++ {
		result = append(result, rand.Float32())
	}

	return FloatVector(result)
}

type FloatVector []float32

func (fv FloatVector) Serialize() []byte {
	data := make([]byte, 0, 4*len(fv)) // float32 occupies 4 bytes
	buf := make([]byte, 4)
	for _, f := range fv {
		binary.LittleEndian.PutUint32(buf, math.Float32bits(f))
		data = append(data, buf...)
	}
	return data
}

func (fv FloatVector) DataType() commonpbv2.PlaceholderType {
	return commonpbv2.PlaceholderType_FloatVector
}

func vector2PlaceholderGroupBytes[T interface {
	Serialize() []byte
	DataType() commonpbv2.PlaceholderType
}](vector T) []byte {
	phg := &commonpbv2.PlaceholderGroup{
		Placeholders: []*commonpbv2.PlaceholderValue{
			vector2Placeholder(vector),
		},
	}

	bs, _ := proto.Marshal(phg)
	return bs
}

func vector2Placeholder[T interface {
	Serialize() []byte
	DataType() commonpbv2.PlaceholderType
}](vector T) *commonpbv2.PlaceholderValue {
	ph := &commonpbv2.PlaceholderValue{
		Tag: "$0",
	}

	ph.Type = vector.DataType()
	ph.Values = append(ph.Values, vector.Serialize())
	return ph
}
