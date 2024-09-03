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
	"github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	rootcoordpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/rootcoordpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type GetConfigurationParam struct {
	framework.ParamBase `use:"show configurations" desc:"iterate all online components and inspect configuration"`
	Format              string `name:"format" default:"line" desc:"output format"`
	DialTimeout         int64  `name:"dialTimeout" default:"2" desc:"grpc dial timeout in seconds"`
	Filter              string `name:"filter" default:"" desc:"configuration key filter sub string"`
}

func (s *InstanceState) GetConfigurationCommand(ctx context.Context, p *GetConfigurationParam) error {
	p.Filter = strings.ToLower(p.Filter)
	sessions, err := common.ListSessions(s.client, s.basePath)
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

		var client configurationSource
		switch strings.ToLower(session.ServerName) {
		case "rootcoord":
			client = rootcoordpbv2.NewRootCoordClient(conn)
		case "datacoord":
			client = datapbv2.NewDataCoordClient(conn)
		case "indexcoord":
			client = indexpbv2.NewIndexCoordClient(conn)
		case "querycoord":
			client = querypbv2.NewQueryCoordClient(conn)
		case "datanode":
			client = datapbv2.NewDataNodeClient(conn)
		case "querynode":
			client = querypbv2.NewQueryNodeClient(conn)
		case "indexnode":
			client = indexpbv2.NewIndexNodeClient(conn)
		}
		if client == nil {
			fmt.Println("client nil", session.String())
			continue
		}

		configurations, err := getConfiguration(ctx, client, session.ServerID)
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
