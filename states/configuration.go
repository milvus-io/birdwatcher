package states

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/mgrpc"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

type GetConfigurationParam struct {
	framework.ParamBase `use:"show configurations" desc:"iterate all online components and inspect configuration"`
	Format              string `name:"format" default:"line" desc:"output format"`
	DialTimeout         int64  `name:"dialTimeout" default:"2" desc:"grpc dial timeout in seconds"`
	Filter              string `name:"filter" default:"" desc:"configuration key filter sub string"`
}

func (s *InstanceState) GetConfigurationCommand(ctx context.Context, p *GetConfigurationParam) error {
	p.Filter = strings.ToLower(p.Filter)
	sessions, err := common.ListSessions(ctx, s.client, s.basePath)
	if err != nil {
		return err
	}

	results := make(map[string]map[string]string)

	for _, session := range sessions {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		}

		var conn *grpc.ClientConn
		var err error
		func() {
			dialCtx, cancel := context.WithTimeout(ctx, time.Duration(p.DialTimeout)*time.Second)
			defer cancel()

			conn, err = grpc.DialContext(dialCtx, session.Address, opts...)
		}()
		if err != nil {
			fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
			continue
		}

		var client mgrpc.ConfigurationSource
		switch strings.ToLower(session.ServerName) {
		case "rootcoord":
			client = rootcoordpb.NewRootCoordClient(conn)
		case "datacoord":
			client = datapb.NewDataCoordClient(conn)
		case "indexcoord":
			client = indexpb.NewIndexCoordClient(conn)
		case "querycoord":
			client = querypb.NewQueryCoordClient(conn)
		case "datanode":
			client = datapb.NewDataNodeClient(conn)
		case "querynode":
			client = querypb.NewQueryNodeClient(conn)
			// case "indexnode":
			// 	client = indexpbv2.NewIndexNodeClient(conn)
		}
		if client == nil {
			fmt.Println("client nil", session.String())
			continue
		}

		configurations, err := mgrpc.GetConfiguration(ctx, client, session.ServerID)
		if err != nil {
			continue
		}

		configurations = lo.Filter(configurations, func(configuration *commonpb.KeyValuePair, _ int) bool {
			return p.Filter == "" || strings.Contains(configuration.GetKey(), p.Filter)
		})

		results[fmt.Sprintf("%s-%d", session.ServerName, session.ServerID)] = common.KVListMap(configurations)
	}

	switch strings.ToLower(p.Format) {
	case "json":
		bs, _ := json.MarshalIndent(results, "", "\t")
		fmt.Println(string(bs))
	case "line":
		fallthrough
	default:
		for comp, configs := range results {
			fmt.Println("Component", comp)
			for key, value := range configs {
				fmt.Printf("%s: %s\n", key, value)
			}
		}
	}
	return nil
}
