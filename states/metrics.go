package states

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

func getFetchMetricsCmd(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fetch-metrics",
		Short: "fetch metrics from milvus instances",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sessions, err := common.ListSessions(ctx, cli, basePath)
			if err != nil {
				fmt.Println("failed to list session", err.Error())
				return
			}

			for _, session := range sessions {
				metrics, defaultMetrics, _ := fetchInstanceMetrics(session)
				fmt.Println(session)
				// TODO parse metrics
				fmt.Println(metrics)
				fmt.Println(defaultMetrics)
			}
		},
	}

	return cmd
}

func fetchInstanceMetrics(session *models.Session) ([]byte, []byte, error) {
	addr := session.Address
	if strings.Contains(session.Address, ":") {
		addr = strings.Split(addr, ":")[0]
	}

	url := fmt.Sprintf("http://%s:%d/metrics", addr, 9091)
	fmt.Println(url)

	// #nosec
	resp, err := http.Get(url)
	if err != nil {
		return nil, nil, err
	}

	metricsBs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	url = fmt.Sprintf("http://%s:%d/metrics_default", addr, 9091)
	// #nosec
	resp, err = http.Get(url)
	if err != nil {
		return nil, nil, err
	}

	defaultMetricsBs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	return metricsBs, defaultMetricsBs, nil
}
