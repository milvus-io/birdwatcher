package mgrpc

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

type ConfigurationSource interface {
	ShowConfigurations(context.Context, *internalpb.ShowConfigurationsRequest, ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error)
}

type MetricsSource interface {
	GetMetrics(context.Context, *milvuspb.GetMetricsRequest, ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error)
}

func GetMetrics(ctx context.Context, client MetricsSource) (string, error) {
	req := &milvuspb.GetMetricsRequest{
		Base:    &commonpb.MsgBase{},
		Request: `{"metric_type": "system_info"}`,
	}
	resp, err := client.GetMetrics(ctx, req)
	return resp.GetResponse(), err
}

func GetConfiguration(ctx context.Context, client ConfigurationSource, id int64) ([]*commonpb.KeyValuePair, error) {
	resp, err := client.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{
		Base: &commonpb.MsgBase{
			SourceID: -1,
			TargetID: id,
		},
	})
	return resp.GetConfiguations(), err
}

func getMetricsCmd(client MetricsSource) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "metrics",
		Short:   "show the metrics provided by current server",
		Aliases: []string{"GetMetrics"},
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := client.GetMetrics(context.Background(), &milvuspb.GetMetricsRequest{
				Base:    &commonpb.MsgBase{},
				Request: `{"metric_type": "system_info"}`,
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("Metrics: %#v\n", resp.Response)
		},
	}

	return cmd
}

func getConfigurationCmd(client ConfigurationSource, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "configuration",
		Short:   "call ShowConfigurations for config inspection",
		Aliases: []string{"GetConfigurations", "configurations"},
		Run: func(cmd *cobra.Command, args []string) {
			prefix, err := cmd.Flags().GetString("prefix")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			resp, err := client.ShowConfigurations(context.Background(), &internalpb.ShowConfigurationsRequest{
				Base: &commonpb.MsgBase{
					SourceID: -1,
					TargetID: id,
				},
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			prefix = strings.ToLower(prefix)
			for _, item := range resp.GetConfiguations() {
				if strings.HasPrefix(item.GetKey(), prefix) {
					fmt.Printf("Key: %s, Value: %s\n", item.Key, item.Value)
				}
			}
		},
	}

	cmd.Flags().String("prefix", "", "the configuration prefix to show")

	return cmd
}
