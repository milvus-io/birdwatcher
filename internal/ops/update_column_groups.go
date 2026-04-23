package ops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// UpdateColumnGroupsParams parses a backfill v2 result JSON and posts one
// UpdateSegmentColumnGroups request per segment to the proxy management
// HTTP endpoint. Mirrors milvus/client/issues/batchupdatev2/main.go.
type UpdateColumnGroupsParams struct {
	BackfillPath string `yaml:"backfill"`
	Addr         string `yaml:"addr"`
	APIKey       string `yaml:"apiKey"`
}

type backfillColumnGroup struct {
	FieldIDs    []int64  `json:"field_ids"`
	BinlogFiles []string `json:"binlog_files"`
	RowCount    int64    `json:"row_count"`
}

type backfillSegmentV2 struct {
	ManifestPaths   []string              `json:"manifestPaths"`
	RowCount        int64                 `json:"rowCount"`
	ExecutionTimeMs int64                 `json:"executionTimeMs"`
	Version         int64                 `json:"version"`
	StorageVersion  int64                 `json:"storage_version"`
	OutputPath      string                `json:"outputPath"`
	ColumnGroups    []backfillColumnGroup `json:"column_groups"`
}

type backfillResultV2 struct {
	NewFieldNames     []string                      `json:"newFieldNames"`
	Success           bool                          `json:"success"`
	CollectionID      int64                         `json:"collectionId"`
	PartitionID       int64                         `json:"partitionId"`
	TotalRowsWritten  int64                         `json:"totalRowsWritten"`
	SegmentsProcessed int                           `json:"segmentsProcessed"`
	CollectionName    string                        `json:"collectionName"`
	Segments          map[string]*backfillSegmentV2 `json:"segments"`
}

type binlogEntry struct {
	EntriesNum int64  `json:"entries_num,omitempty"`
	LogID      int64  `json:"logID,omitempty"`
	LogPath    string `json:"log_path,omitempty"`
	LogSize    int64  `json:"log_size,omitempty"`
	MemorySize int64  `json:"memory_size,omitempty"`
}

type fieldBinlog struct {
	FieldID     int64          `json:"fieldID"`
	Binlogs     []*binlogEntry `json:"binlogs"`
	ChildFields []int64        `json:"child_fields,omitempty"`
}

type updateColumnGroupsRequest struct {
	SegmentID    int64                   `json:"segment_id"`
	ColumnGroups map[string]*fieldBinlog `json:"column_groups"`
}

func parseLogID(path string) int64 {
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return 0
	}
	id, err := strconv.ParseInt(path[idx+1:], 10, 64)
	if err != nil {
		return 0
	}
	return id
}

func (p *UpdateColumnGroupsParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if p.BackfillPath == "" {
		return nil, fmt.Errorf("update_column_groups: `backfill` is required")
	}
	if p.Addr == "" {
		return nil, fmt.Errorf("update_column_groups: `addr` is required (proxy management HTTP address, e.g. http://host:9091)")
	}

	raw, err := os.ReadFile(p.BackfillPath)
	if err != nil {
		return nil, fmt.Errorf("read backfill file: %w", err)
	}
	result := &backfillResultV2{}
	if err := json.Unmarshal(raw, result); err != nil {
		return nil, fmt.Errorf("parse backfill json: %w", err)
	}
	if !result.Success {
		return nil, fmt.Errorf("backfill result is not successful, aborting")
	}
	fmt.Fprintf(rc.Out(), "backfill fields: %v, segments: %d\n", result.NewFieldNames, len(result.Segments))

	url := strings.TrimRight(p.Addr, "/") + "/management/datacoord/segment/column_groups/update"

	updated := 0
	for segIDStr, seg := range result.Segments {
		segID, err := strconv.ParseInt(segIDStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid segment id %q: %w", segIDStr, err)
		}
		if len(seg.ColumnGroups) == 0 {
			fmt.Fprintf(rc.Out(), "segment %d: no column_groups, skipping\n", segID)
			continue
		}

		req := &updateColumnGroupsRequest{
			SegmentID:    segID,
			ColumnGroups: make(map[string]*fieldBinlog, len(seg.ColumnGroups)),
		}
		for _, cg := range seg.ColumnGroups {
			if len(cg.FieldIDs) == 0 {
				return nil, fmt.Errorf("segment %d: empty field_ids in column group", segID)
			}
			groupID := cg.FieldIDs[0]
			fb := &fieldBinlog{
				FieldID:     groupID,
				ChildFields: cg.FieldIDs,
			}
			for _, path := range cg.BinlogFiles {
				fb.Binlogs = append(fb.Binlogs, &binlogEntry{
					EntriesNum: cg.RowCount,
					LogID:      parseLogID(path),
				})
			}
			req.ColumnGroups[strconv.FormatInt(groupID, 10)] = fb
		}

		body, err := json.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		fmt.Fprintf(rc.Out(), "segment %d -> POST %s\n", segID, url)

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("build http request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if p.APIKey != "" {
			httpReq.Header.Set("Authorization", "Bearer "+p.APIKey)
		}

		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("segment %d: http request failed: %w", segID, err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("segment %d: update failed, status=%d body=%s", segID, resp.StatusCode, string(respBody))
		}
		fmt.Fprintf(rc.Out(), "segment %d: update success, resp=%s\n", segID, string(respBody))
		updated++
	}

	fmt.Fprintf(rc.Out(), "UpdateSegmentColumnGroups done for %d segments\n", updated)
	return map[string]any{
		"collection": result.CollectionName,
		"segments":   updated,
	}, nil
}

func init() {
	Register("update_column_groups", func() Op { return &UpdateColumnGroupsParams{} })
}
