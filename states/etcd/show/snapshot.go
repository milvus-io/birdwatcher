package show

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
)

type SnapshotParam struct {
	framework.DataSetParam `use:"show snapshots" desc:"display snapshot metadata from data coord meta store" alias:"snapshot"`
	CollectionID           int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	SnapshotID             int64  `name:"snapshot" default:"0" desc:"snapshot id to display"`
	State                  string `name:"state" default:"" desc:"snapshot state to filter"`
}

func (c *ComponentShow) SnapshotCommand(ctx context.Context, p *SnapshotParam) (*framework.PresetResultSet, error) {
	snapshots, err := common.ListSnapshotsBy(ctx, c.client, c.metaPath, common.SnapshotSelector{
		CollectionID: p.CollectionID,
		SnapshotID:   p.SnapshotID,
		Filters: []common.PostFilter[models.Snapshot]{
			func(snapshot *models.Snapshot) bool {
				return p.State == "" || strings.EqualFold(snapshot.GetProto().GetState().String(), p.State)
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	if p.SnapshotID > 0 && len(snapshots) == 0 {
		return nil, fmt.Errorf("snapshot %d not found", p.SnapshotID)
	}

	return framework.NewPresetResultSet(framework.NewListResult[Snapshots](snapshots), framework.NameFormat(p.Format)), nil
}

type Snapshots struct {
	framework.ListResultSet[*models.Snapshot]
}

func (rs *Snapshots) TableHeaders() table.Row {
	return table.Row{"SnapshotID", "Name", "CollectionID", "State", "Partitions", "CreateTS", "S3Location"}
}

func (rs *Snapshots) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(rs.Data))
	for _, snapshot := range rs.Data {
		info := snapshot.GetProto()
		rows = append(rows, table.Row{
			info.GetId(),
			info.GetName(),
			info.GetCollectionId(),
			info.GetState().String(),
			len(info.GetPartitionIds()),
			formatHybridTS(info.GetCreateTs()),
			info.GetS3Location(),
		})
	}
	return rows
}

func (rs *Snapshots) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		return rs.printDefault()
	case framework.FormatJSON:
		return rs.printAsJSON()
	default:
	}
	return ""
}

func (rs *Snapshots) printDefault() string {
	sb := &strings.Builder{}
	for _, snapshot := range rs.Data {
		rs.printSnapshot(sb, snapshot)
	}
	fmt.Fprintf(sb, "--- Total Snapshot(s): %d\n", len(rs.Data))
	return sb.String()
}

func (rs *Snapshots) printAsJSON() string {
	type SnapshotJSON struct {
		Key                   string          `json:"key"`
		Name                  string          `json:"name"`
		SnapshotID            int64           `json:"snapshot_id"`
		Description           string          `json:"description,omitempty"`
		CollectionID          int64           `json:"collection_id"`
		PartitionIDs          []int64         `json:"partition_ids,omitempty"`
		CreateTS              int64           `json:"create_ts"`
		CreateTime            string          `json:"create_time,omitempty"`
		CreateTSLogical       uint64          `json:"create_ts_logical,omitempty"`
		S3Location            string          `json:"s3_location,omitempty"`
		State                 string          `json:"state"`
		PendingStartTime      int64           `json:"pending_start_time"`
		PendingStartTimeHuman string          `json:"pending_start_time_human,omitempty"`
		CompactionExpireTime  uint64          `json:"compaction_expire_time"`
		CompactionExpireHuman string          `json:"compaction_expire_time_human,omitempty"`
		PinIDs                []int64         `json:"pin_ids,omitempty"`
		PinExpireAtMs         map[int64]int64 `json:"pin_expire_at_ms,omitempty"`
	}

	type OutputJSON struct {
		Snapshots    []SnapshotJSON `json:"snapshots"`
		MatchedCount int            `json:"matched_count"`
	}

	output := OutputJSON{
		Snapshots:    make([]SnapshotJSON, 0, len(rs.Data)),
		MatchedCount: len(rs.Data),
	}

	for _, snapshot := range rs.Data {
		info := snapshot.GetProto()
		createTime, logical := utils.ParseTS(uint64(info.GetCreateTs()))
		item := SnapshotJSON{
			Key:                  snapshot.Key(),
			Name:                 info.GetName(),
			SnapshotID:           info.GetId(),
			Description:          info.GetDescription(),
			CollectionID:         info.GetCollectionId(),
			PartitionIDs:         append([]int64(nil), info.GetPartitionIds()...),
			CreateTS:             info.GetCreateTs(),
			CreateTime:           createTime.Format(tsPrintFormat),
			CreateTSLogical:      logical,
			S3Location:           info.GetS3Location(),
			State:                info.GetState().String(),
			PendingStartTime:     info.GetPendingStartTime(),
			CompactionExpireTime: info.GetCompactionExpireTime(),
			PinIDs:               append([]int64(nil), info.GetPinIds()...),
			PinExpireAtMs:        cloneInt64Map(info.GetPinExpireAtMs()),
		}
		if info.GetPendingStartTime() > 0 {
			item.PendingStartTimeHuman = time.UnixMilli(info.GetPendingStartTime()).Format(tsPrintFormat)
		}
		if info.GetCompactionExpireTime() > 0 {
			item.CompactionExpireHuman = time.Unix(int64(info.GetCompactionExpireTime()), 0).Format(tsPrintFormat)
		}
		output.Snapshots = append(output.Snapshots, item)
	}

	return framework.MarshalJSON(output)
}

func (rs *Snapshots) printSnapshot(sb *strings.Builder, snapshot *models.Snapshot) {
	info := snapshot.GetProto()
	createTime, logical := utils.ParseTS(uint64(info.GetCreateTs()))

	fmt.Fprintln(sb, "=============================")
	fmt.Fprintf(sb, "Key: %s\n", snapshot.Key())
	fmt.Fprintf(sb, "Snapshot ID: %d\tName: %s\n", info.GetId(), info.GetName())
	fmt.Fprintf(sb, "CollectionID: %d\tState: %s\n", info.GetCollectionId(), info.GetState().String())
	if description := info.GetDescription(); description != "" {
		fmt.Fprintf(sb, "Description: %s\n", description)
	}
	fmt.Fprintf(sb, "Create TS: %d\tTime: %s\tLogical: %d\n", info.GetCreateTs(), createTime.Format(tsPrintFormat), logical)
	fmt.Fprintf(sb, "Partitions(%d): %v\n", len(info.GetPartitionIds()), info.GetPartitionIds())
	fmt.Fprintf(sb, "S3 Location: %s\n", emptyAsNA(info.GetS3Location()))
	fmt.Fprintf(sb, "Pending Start Time: %s\n", formatUnixMillis(info.GetPendingStartTime()))
	fmt.Fprintf(sb, "Compaction Expire Time: %s\n", formatUnixSeconds(info.GetCompactionExpireTime()))
	fmt.Fprintf(sb, "Pin IDs(%d): %v\n", len(info.GetPinIds()), info.GetPinIds())
	fmt.Fprintf(sb, "Pin Expire At Ms(%d):\n", len(info.GetPinExpireAtMs()))

	pinIDs := make([]int64, 0, len(info.GetPinExpireAtMs()))
	for pinID := range info.GetPinExpireAtMs() {
		pinIDs = append(pinIDs, pinID)
	}
	slices.Sort(pinIDs)
	for _, pinID := range pinIDs {
		expireAt := info.GetPinExpireAtMs()[pinID]
		fmt.Fprintf(sb, "  - %d: %s\n", pinID, formatUnixMillis(expireAt))
	}
}

func cloneInt64Map(src map[int64]int64) map[int64]int64 {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[int64]int64, len(src))
	maps.Copy(dst, src)
	return dst
}

func formatHybridTS(ts int64) string {
	if ts <= 0 {
		return "N/A"
	}
	t, logical := utils.ParseTS(uint64(ts))
	return fmt.Sprintf("%s (logical=%d)", t.Format(tsPrintFormat), logical)
}

func formatUnixMillis(ts int64) string {
	if ts <= 0 {
		return "N/A"
	}
	return time.UnixMilli(ts).Format(tsPrintFormat)
}

func formatUnixSeconds(ts uint64) string {
	if ts == 0 {
		return "N/A"
	}
	return time.Unix(int64(ts), 0).Format(tsPrintFormat)
}

func emptyAsNA(value string) string {
	if value == "" {
		return "N/A"
	}
	return value
}
