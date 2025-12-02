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

		if result.stats.success {
			// Calculate fragmentation: (active - allocated) / active
			fragmentationPercent := 0.0
			if result.stats.active > 0 {
				fragmentationPercent = float64(result.stats.active-result.stats.allocated) / float64(result.stats.active) * 100
			}

			// Calculate overhead: (resident - active) / active
			overheadPercent := 0.0
			if result.stats.active > 0 {
				overheadPercent = float64(result.stats.resident-result.stats.active) / float64(result.stats.active) * 100
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintf(w, "  Jemalloc Allocated:\t%s\t(bytes allocated by the application)\n", formatBytes(result.stats.allocated))
			fmt.Fprintf(w, "  Jemalloc Active:\t%s\t(bytes in active pages)\n", formatBytes(result.stats.active))
			fmt.Fprintf(w, "  Jemalloc Metadata:\t%s\t(bytes for internal metadata)\n", formatBytes(result.stats.metadata))
			fmt.Fprintf(w, "  Jemalloc Resident:\t%s\t(resident physical memory)\n", formatBytes(result.stats.resident))
			fmt.Fprintf(w, "  Jemalloc Mapped:\t%s\t(total mapped memory)\n", formatBytes(result.stats.mapped))
			fmt.Fprintf(w, "  Jemalloc Retained:\t%s\t(retained by allocator)\n", formatBytes(result.stats.retained))
			fmt.Fprintf(w, "  Jemalloc Fragmentation:\t%.2f%%\t(internal fragmentation)\n", fragmentationPercent)
			fmt.Fprintf(w, "  Jemalloc Overhead:\t%.2f%%\t(memory overhead)\n", overheadPercent)
			w.Flush()
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
	success       bool
	allocated     uint64
	active        uint64
	metadata      uint64
	resident      uint64
	mapped        uint64
	retained      uint64
	fragmentation float64
	overhead      float64
	memoryUsage   uint64
	memory        uint64
	rawJSON       string
}

// metricsResponse represents the structure of GetMetrics response
type metricsResponse struct {
	Name          string          `json:"name"`
	Type          string          `json:"type"`
	HardwareInfos hardwareMetrics `json:"hardware_infos"`
}

type hardwareMetrics struct {
	IP                    string  `json:"ip"`
	CPUCoreCount          int     `json:"cpu_core_count"`
	CPUCoreUsage          float64 `json:"cpu_core_usage"`
	Memory                uint64  `json:"memory"`
	MemoryUsage           uint64  `json:"memory_usage"`
	JemallocAllocated     uint64  `json:"jemalloc_allocated"`
	JemallocActive        uint64  `json:"jemalloc_active"`
	JemallocMetadata      uint64  `json:"jemalloc_metadata"`
	JemallocResident      uint64  `json:"jemalloc_resident"`
	JemallocMapped        uint64  `json:"jemalloc_mapped"`
	JemallocRetained      uint64  `json:"jemalloc_retained"`
	JemallocFragmentation float64 `json:"jemalloc_fragmentation"`
	JemallocOverhead      float64 `json:"jemalloc_overhead"`
	JemallocSuccess       bool    `json:"jemalloc_success"`
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
		success:       metrics.HardwareInfos.JemallocSuccess,
		allocated:     metrics.HardwareInfos.JemallocAllocated,
		active:        metrics.HardwareInfos.JemallocActive,
		metadata:      metrics.HardwareInfos.JemallocMetadata,
		resident:      metrics.HardwareInfos.JemallocResident,
		mapped:        metrics.HardwareInfos.JemallocMapped,
		retained:      metrics.HardwareInfos.JemallocRetained,
		fragmentation: metrics.HardwareInfos.JemallocFragmentation,
		overhead:      metrics.HardwareInfos.JemallocOverhead,
		memoryUsage:   metrics.HardwareInfos.MemoryUsage,
		memory:        metrics.HardwareInfos.Memory,
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
