package states

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"text/tabwriter"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

// JemallocStatsParam defines parameters for jemalloc-stats command
type JemallocStatsParam struct {
	framework.ParamBase `use:"jemalloc-stats" desc:"get jemalloc memory statistics from Milvus nodes"`
	Full                bool   `name:"full" default:"false" desc:"print full system info JSON"`
	NodeType            string `name:"nodeType" default:"" desc:"filter by node type (querynode, datanode, etc)"`
}

// JemallocStatsCommand implements jemalloc-stats command
func (s *InstanceState) JemallocStatsCommand(ctx context.Context, p *JemallocStatsParam) error {
	sessions, err := common.ListSessions(ctx, s.client, s.basePath)
	if err != nil {
		return errors.Wrap(err, "failed to list sessions")
	}

	// Filter by node type if specified
	if p.NodeType != "" {
		sessions = lo.Filter(sessions, func(session *models.Session, _ int) bool {
			return session.ServerName == p.NodeType
		})
	}

	if len(sessions) == 0 {
		fmt.Println("No matching nodes found")
		return nil
	}

	// Dedup by serverID + serverName (standalone sessions share same serverID but have different server names)
	groups := lo.GroupBy(sessions, func(session *models.Session) string {
		return fmt.Sprintf("%d-%s", session.ServerID, session.ServerName)
	})

	type jemallocResult struct {
		session *models.Session
		stats   *jemallocStats
		err     error
	}

	ch := make(chan jemallocResult, len(groups))

	wg := sync.WaitGroup{}
	wg.Add(len(groups))

	for _, sessionGroup := range groups {
		go func(sessions []*models.Session) {
			defer wg.Done()
			if len(sessions) == 0 {
				return
			}

			session := sessions[0]
			stats, err := fetchJemallocStats(ctx, session, p.Full)
			ch <- jemallocResult{
				session: session,
				stats:   stats,
				err:     err,
			}
		}(sessionGroup)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	// Collect results
	var results []jemallocResult
	for result := range ch {
		results = append(results, result)
	}

	// Print results
	if p.Full {
		for _, result := range results {
			fmt.Printf("\n=== %s-%d (%s) ===\n", result.session.ServerName, result.session.ServerID, result.session.Address)
			if result.err != nil {
				fmt.Printf("Error: %v\n", result.err)
				continue
			}
			if result.stats != nil && result.stats.rawJSON != "" {
				fmt.Println(result.stats.rawJSON)
			}
		}
		return nil
	}

	// Print formatted table
	fmt.Println()
	fmt.Println(repeatString("=", 80))
	fmt.Println("Jemalloc Memory Statistics")
	fmt.Println(repeatString("=", 80))

	for _, result := range results {
		fmt.Printf("\nNode: %s-%d (%s)\n", result.session.ServerName, result.session.ServerID, result.session.Address)
		fmt.Println(repeatString("-", 50))

		if result.err != nil {
			fmt.Printf("  Error: %v\n", result.err)
			continue
		}

		if result.stats == nil {
			fmt.Println("  No stats available")
			continue
		}

		if result.stats.available {
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintf(w, "  Jemalloc Allocated:\t%s\t(actual app usage)\n", formatBytes(result.stats.allocated))
			fmt.Fprintf(w, "  Jemalloc Resident:\t%s\t(physical memory)\n", formatBytes(result.stats.resident))
			fmt.Fprintf(w, "  Jemalloc Cached:\t%s\t(unreturned to OS)\n", formatBytes(result.stats.cached))
			fmt.Fprintf(w, "  Jemalloc Available:\t%v\n", result.stats.available)
			w.Flush()

			if result.stats.resident > 0 {
				cacheRatio := float64(result.stats.cached) / float64(result.stats.resident) * 100
				fmt.Printf("  Cache Ratio:        %6.1f%%  (cached/resident)\n", cacheRatio)
			}
		} else {
			fmt.Println("  Jemalloc stats not available for this node")
			if result.stats.memoryUsage > 0 || result.stats.memory > 0 {
				fmt.Printf("  Memory Usage: %s\n", formatBytes(result.stats.memoryUsage))
				fmt.Printf("  Total Memory: %s\n", formatBytes(result.stats.memory))
			}
		}
	}

	fmt.Println()
	fmt.Println(repeatString("=", 80))

	return nil
}

type jemallocStats struct {
	available   bool
	allocated   uint64
	resident    uint64
	cached      uint64
	memoryUsage uint64
	memory      uint64
	rawJSON     string
}

// metricsResponse represents the structure of GetMetrics response
type metricsResponse struct {
	Name          string          `json:"name"`
	Type          string          `json:"type"`
	HardwareInfos hardwareMetrics `json:"hardware_infos"`
}

type hardwareMetrics struct {
	IP                string  `json:"ip"`
	CPUCoreCount      int     `json:"cpu_core_count"`
	CPUCoreUsage      float64 `json:"cpu_core_usage"`
	Memory            uint64  `json:"memory"`
	MemoryUsage       uint64  `json:"memory_usage"`
	JemallocAvailable bool    `json:"jemalloc_available"`
	JemallocAllocated uint64  `json:"jemalloc_allocated"`
	JemallocResident  uint64  `json:"jemalloc_resident"`
	JemallocCached    uint64  `json:"jemalloc_cached"`
}

func fetchJemallocStats(ctx context.Context, session *models.Session, full bool) (*jemallocStats, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	conn, err := grpc.DialContext(ctx, session.Address, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect")
	}
	defer conn.Close()

	// Create GetMetrics request
	req := &milvuspb.GetMetricsRequest{
		Base:    &commonpb.MsgBase{},
		Request: `{"metric_type": "system_info"}`,
	}

	var respStr string

	// Call GetMetrics based on server type
	switch session.ServerName {
	case "querynode":
		client := querypb.NewQueryNodeClient(conn)
		resp, err := client.GetMetrics(ctx, req)
		if err != nil {
			return nil, errors.Wrap(err, "GetMetrics failed")
		}
		respStr = resp.GetResponse()
	case "querycoord", "mixcoord":
		client := querypb.NewQueryCoordClient(conn)
		resp, err := client.GetMetrics(ctx, req)
		if err != nil {
			return nil, errors.Wrap(err, "GetMetrics failed")
		}
		respStr = resp.GetResponse()
	case "datanode":
		client := datapb.NewDataNodeClient(conn)
		resp, err := client.GetMetrics(ctx, req)
		if err != nil {
			return nil, errors.Wrap(err, "GetMetrics failed")
		}
		respStr = resp.GetResponse()
	case "datacoord":
		client := datapb.NewDataCoordClient(conn)
		resp, err := client.GetMetrics(ctx, req)
		if err != nil {
			return nil, errors.Wrap(err, "GetMetrics failed")
		}
		respStr = resp.GetResponse()
	case "rootcoord":
		client := rootcoordpb.NewRootCoordClient(conn)
		resp, err := client.GetMetrics(ctx, req)
		if err != nil {
			return nil, errors.Wrap(err, "GetMetrics failed")
		}
		respStr = resp.GetResponse()
	default:
		return nil, errors.Errorf("unsupported server type: %s", session.ServerName)
	}

	if respStr == "" {
		return nil, errors.New("empty response from GetMetrics")
	}

	// Parse response
	var metrics metricsResponse
	if err := json.Unmarshal([]byte(respStr), &metrics); err != nil {
		return nil, errors.Wrap(err, "failed to parse metrics response")
	}

	stats := &jemallocStats{
		available:   metrics.HardwareInfos.JemallocAvailable,
		allocated:   metrics.HardwareInfos.JemallocAllocated,
		resident:    metrics.HardwareInfos.JemallocResident,
		cached:      metrics.HardwareInfos.JemallocCached,
		memoryUsage: metrics.HardwareInfos.MemoryUsage,
		memory:      metrics.HardwareInfos.Memory,
	}

	if full {
		// Pretty print the raw JSON
		var prettyJSON map[string]interface{}
		if err := json.Unmarshal([]byte(respStr), &prettyJSON); err == nil {
			prettyBytes, _ := json.MarshalIndent(prettyJSON, "", "  ")
			stats.rawJSON = string(prettyBytes)
		} else {
			stats.rawJSON = respStr
		}
	}

	return stats, nil
}

func formatBytes(sizeBytes uint64) string {
	if sizeBytes == 0 {
		return "0 B"
	}

	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	size := float64(sizeBytes)

	for _, unit := range units {
		if size < 1024.0 {
			return fmt.Sprintf("%.2f %s", size, unit)
		}
		size /= 1024.0
	}
	return fmt.Sprintf("%.2f PB", size)
}

func repeatString(s string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += s
	}
	return result
}
