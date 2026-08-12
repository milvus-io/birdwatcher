package show

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/samber/lo"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

type WALRecoveryStorageParam struct {
	framework.ParamBase `use:"show wal-recovery-storage" desc:"display wal recovery storage information from coordinator meta store"`
	Channel             string `name:"channel" default:"" desc:"phsical channel or vchannel name to filter"`
	WALName             string `name:"wal-name" default:"" desc:"a hint of wal name, auto guess if not provided"`
	Detail              bool   `name:"detail" default:"false" desc:"show detailed growing segment and schema information"`
}

func (c *ComponentShow) WalRecoveryStorageCommand(ctx context.Context, p *WALRecoveryStorageParam) error {
	if p.Channel == "" {
		return errors.Errorf("channel is required, use --channel to specify a physical channel or virtual channel")
	}
	pchannel := funcutil.ToPhysicalChannel(p.Channel)

	metas, err := common.ListWALRecoveryStorage(ctx, c.client, c.metaPath, pchannel)
	if err != nil {
		return err
	}

	sessions, err := common.ListSessions(ctx, c.client, c.metaPath)
	if err != nil {
		return err
	}
	sessionMap := lo.SliceToMap(sessions, func(s *models.Session) (int64, *models.Session) { return s.ServerID, s })

	nodeInfo := types.NewStreamingNodeInfoFromProto(metas.Channel.Node)

	if p.Detail {
		return printWALRecoveryStorageDetail(metas, p, nodeInfo, sessionMap)
	}
	return printWALRecoveryStorageSummary(metas, p, nodeInfo, sessionMap)
}

func printWALRecoveryStorageSummary(metas *common.RecoveryStorageMeta, p *WALRecoveryStorageParam, nodeInfo types.StreamingNodeInfo, sessionMap map[int64]*models.Session) error {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetTitle(fmt.Sprintf("WAL Recovery Storage: %s, At StreamingNode: %s, Checkpoints: %s",
		types.NewPChannelInfoFromProto(metas.Channel.Channel).String(),
		formatStreamingNode(nodeInfo, sessionMap),
		common.FormatWALCheckpoint(p.WALName, metas.Checkpoints)))
	t.AppendHeader(table.Row{"Channel", "Partition Count", "Segment Count", "Segments", "SchemaVersion"})
	for _, meta := range metas.VChannels {
		if !strings.HasPrefix(meta.Vchannel, p.Channel) {
			continue
		}
		segments := make([]string, 0, len(metas.Segments[meta.Vchannel]))
		for _, segment := range metas.Segments[meta.Vchannel] {
			segments = append(segments, common.FormatSegmentAssignmentMeta(segment))
		}
		schemas := make([]string, 0, len(meta.CollectionInfo.Schemas))
		for _, schema := range meta.CollectionInfo.Schemas {
			schemas = append(schemas, common.FormatSchema(schema))
		}
		t.AppendRow(table.Row{
			meta.Vchannel,
			fmt.Sprintf("%d", len(meta.CollectionInfo.Partitions)),
			fmt.Sprintf("%d", len(metas.Segments[meta.Vchannel])),
			strings.Join(segments, "\n"),
			strings.Join(schemas, "\n"),
		})
	}
	t.Render()

	if len(metas.RedundantSchemas) > 0 {
		t.AppendHeader(table.Row{"Redundant Schemas"})
		for _, schemas := range metas.RedundantSchemas {
			for _, schema := range schemas {
				json, err := protojson.Marshal(schema)
				if err != nil {
					return err
				}
				t.AppendRow(table.Row{string(json)})
			}
		}
	}

	if len(metas.RedundantSegments) > 0 {
		t.AppendHeader(table.Row{"Redundant Segments"})
		for _, segments := range metas.RedundantSegments {
			for _, segment := range segments {
				json, err := protojson.Marshal(segment)
				if err != nil {
					return err
				}
				t.AppendRow(table.Row{string(json)})
			}
		}
	}
	return nil
}

func printWALRecoveryStorageDetail(metas *common.RecoveryStorageMeta, p *WALRecoveryStorageParam, nodeInfo types.StreamingNodeInfo, sessionMap map[int64]*models.Session) error {
	sb := &strings.Builder{}

	fmt.Fprintf(sb, "================================================================================\n")
	fmt.Fprintf(sb, "WAL Recovery Storage Detail\n")
	fmt.Fprintf(sb, "================================================================================\n")
	fmt.Fprintf(sb, "PChannel:       %s\n", types.NewPChannelInfoFromProto(metas.Channel.Channel).String())
	fmt.Fprintf(sb, "StreamingNode:  %s\n", formatStreamingNode(nodeInfo, sessionMap))
	fmt.Fprintf(sb, "Checkpoint:     %s\n", common.FormatWALCheckpoint(p.WALName, metas.Checkpoints))
	fmt.Fprintf(sb, "\n")

	for _, meta := range metas.VChannels {
		if !strings.HasPrefix(meta.Vchannel, p.Channel) {
			continue
		}

		segments := metas.Segments[meta.Vchannel]
		schemas := meta.CollectionInfo.Schemas

		fmt.Fprintf(sb, "--------------------------------------------------------------------------------\n")
		fmt.Fprintf(sb, "VChannel: %s\n", meta.Vchannel)
		fmt.Fprintf(sb, "  State:           %s\n", meta.State.String())
		fmt.Fprintf(sb, "  CollectionID:    %d\n", meta.CollectionInfo.CollectionId)
		fmt.Fprintf(sb, "  Checkpoint Tick: %d\n", meta.CheckpointTimeTick)
		fmt.Fprintf(sb, "  Partitions:      %d\n", len(meta.CollectionInfo.Partitions))
		for _, partition := range meta.CollectionInfo.Partitions {
			fmt.Fprintf(sb, "    - PartitionID: %d\n", partition.PartitionId)
		}

		// Segments detail
		fmt.Fprintf(sb, "\n  Growing Segments: %d\n", len(segments))
		if len(segments) > 0 {
			t := table.NewWriter()
			t.SetStyle(table.StyleLight)
			t.AppendHeader(table.Row{"SegmentID", "State", "Level", "Rows", "BinarySize", "MaxBinarySize", "Binlogs", "CreateTick", "CheckpointTick", "CreateTime", "LastModified"})
			for _, seg := range segments {
				stat := seg.Stat
				createTime := formatTimestamp(stat.CreateTimestamp)
				lastModified := formatTimestamp(stat.LastModifiedTimestamp)
				t.AppendRow(table.Row{
					seg.SegmentId,
					seg.State.String(),
					stat.Level.String(),
					stat.ModifiedRows,
					formatBytes(stat.ModifiedBinarySize),
					formatBytes(stat.MaxBinarySize),
					stat.BinlogCounter,
					stat.CreateSegmentTimeTick,
					seg.CheckpointTimeTick,
					createTime,
					lastModified,
				})
			}
			fmt.Fprintf(sb, "%s\n", t.Render())
		}

		// Schema detail
		fmt.Fprintf(sb, "\n  Schemas: %d\n", len(schemas))
		for idx, schema := range schemas {
			fmt.Fprintf(sb, "\n  [Schema %d] tick=%d  state=%s\n", idx, schema.CheckpointTimeTick, schema.State.String())
			collSchema := schema.Schema
			if collSchema == nil {
				fmt.Fprintf(sb, "    (nil schema)\n")
				continue
			}
			fmt.Fprintf(sb, "    Collection:        %s\n", collSchema.Name)
			fmt.Fprintf(sb, "    EnableDynamicField: %v\n", collSchema.EnableDynamicField)

			fields := collSchema.Fields
			sort.Slice(fields, func(i, j int) bool {
				return fields[i].FieldID < fields[j].FieldID
			})
			fmt.Fprintf(sb, "    Fields: (%d)\n", len(fields))
			for _, field := range fields {
				fmt.Fprintf(sb, "      - FieldID: %-6d  Name: %-30s  Type: %s\n", field.FieldID, field.Name, field.DataType.String())
				printFieldAttributes(sb, field, "        ")
			}
		}
		fmt.Fprintf(sb, "\n")
	}

	// Redundant info
	if len(metas.RedundantSchemas) > 0 {
		fmt.Fprintf(sb, "--------------------------------------------------------------------------------\n")
		fmt.Fprintf(sb, "Redundant Schemas (orphaned, no matching vchannel):\n")
		for vchannel, schemas := range metas.RedundantSchemas {
			for _, schema := range schemas {
				fmt.Fprintf(sb, "  vchannel=%s  tick=%d  state=%s\n", vchannel, schema.CheckpointTimeTick, schema.State.String())
			}
		}
	}

	if len(metas.RedundantSegments) > 0 {
		fmt.Fprintf(sb, "--------------------------------------------------------------------------------\n")
		fmt.Fprintf(sb, "Redundant Segments (orphaned, no matching vchannel):\n")
		for vchannel, segments := range metas.RedundantSegments {
			for _, seg := range segments {
				fmt.Fprintf(sb, "  vchannel=%s  segmentID=%d  state=%s  rows=%d\n", vchannel, seg.SegmentId, seg.State.String(), seg.Stat.ModifiedRows)
			}
		}
	}

	fmt.Print(sb.String())
	return nil
}

func printFieldAttributes(sb *strings.Builder, field *schemapb.FieldSchema, indent string) {
	if field.IsPrimaryKey {
		fmt.Fprintf(sb, "%sPrimary Key, AutoID: %t\n", indent, field.AutoID)
	}
	if field.GetNullable() {
		fmt.Fprintf(sb, "%s%s\n", indent, color.MagentaString("Nullable"))
	}
	if field.GetDefaultValue() != nil {
		fmt.Fprintf(sb, "%s%s: %v\n", indent, color.MagentaString("DefaultValue"), field.GetDefaultValue())
	}
	if field.IsDynamic {
		fmt.Fprintf(sb, "%sDynamic Field\n", indent)
	}
	if field.IsPartitionKey {
		fmt.Fprintf(sb, "%sPartition Key\n", indent)
	}
	if field.IsClusteringKey {
		fmt.Fprintf(sb, "%sClustering Key\n", indent)
	}
	if field.IsFunctionOutput {
		fmt.Fprintf(sb, "%sFunction Output\n", indent)
	}
	if field.DataType == schemapb.DataType_Array {
		fmt.Fprintf(sb, "%sElement Type: %s\n", indent, field.ElementType.String())
	}
	for _, kv := range field.TypeParams {
		fmt.Fprintf(sb, "%sType Param %s: %s\n", indent, kv.Key, kv.Value)
	}
}

func formatTimestamp(ts int64) string {
	if ts == 0 {
		return "-"
	}
	return time.Unix(ts, 0).Format("2006-01-02 15:04:05")
}

func formatBytes(size uint64) string {
	if size < 1024 {
		return fmt.Sprintf("%dB", size)
	}
	if size < 1024*1024 {
		return fmt.Sprintf("%.1fKB", float64(size)/1024)
	}
	if size < 1024*1024*1024 {
		return fmt.Sprintf("%.1fMB", float64(size)/(1024*1024))
	}
	return fmt.Sprintf("%.1fGB", float64(size)/(1024*1024*1024))
}
