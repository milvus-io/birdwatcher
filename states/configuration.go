package states

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	rootcoordpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/rootcoordpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// GetConfigurationCommand returns command to iterate all online components and fetch configurations.
func GetConfigurationCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "configurations",
		Short: "iterate all online components and inspect configuration",
		Run: func(cmd *cobra.Command, args []string) {
			format, err := cmd.Flags().GetString("format")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			sessions, err := common.ListSessions(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			results := make(map[string]map[string]string)

			for _, session := range sessions {
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
					continue
				}

				configurations, err := getConfiguration(context.Background(), client, session.ServerID)
				if err != nil {
					continue
				}

				results[fmt.Sprintf("%s-%d", session.ServerName, session.ServerID)] = common.KVListMap(configurations)
			}

			switch strings.ToLower(format) {
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

		},
	}

	cmd.Flags().String("format", "line", "output format")
	return cmd
}
