package show

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

// droppedChannelCheckpointTimestamp is funcutil.DroppedChannelCheckpointTimestamp in
// Milvus: the reserved value DataCoord writes into a channel checkpoint once the channel
// is dropped. It decodes to the year 4199, so it must never be taken as a progress reading.
const droppedChannelCheckpointTimestamp = uint64(math.MaxUint64)

// usableProgress reports whether ts can be used as a reading of how far the cluster has
// progressed. The dropped-channel sentinel and anything dated in the future are not:
// letting either become the reference makes every other vchannel look arbitrarily stale.
func usableProgress(ts uint64, now time.Time) bool {
	if ts == 0 || ts == droppedChannelCheckpointTimestamp {
		return false
	}
	t, _ := utils.ParseTS(ts)
	return !t.After(now.Add(time.Minute))
}

// VChannelHealthParam is the parameter of `show vchannel-health`.
type VChannelHealthParam struct {
	framework.DataSetParam `use:"show vchannel-health" desc:"find vchannels whose flush checkpoint stopped advancing; any such vchannel keeps GetFlushAllState false and makes flushAll / milvus-backup create wait forever" alias:"vchannel-health,vch"`
	CollectionID           int64 `name:"collection" default:"0" desc:"collection id to filter with, 0 for all"`
	LagMinutes             int64 `name:"lag" default:"10" desc:"minutes behind the pchannel's consume checkpoint (or the newest checkpoint in the cluster) to flag a vchannel as FROZEN"`
	All                    bool  `name:"all" default:"false" desc:"list every vchannel, not only the flagged ones"`
}

// VChannelHealth is one row of the report.
type VChannelHealth struct {
	VChannel   string
	PChannel   string
	Collection string // db.name
	CollState  string
	Checkpoint time.Time
	// LagMinutes is how far the checkpoint is behind the reference: the pchannel's consume checkpoint,
	// or the newest checkpoint in the cluster when the pchannel has none.
	LagMinutes float64
	// PChannelConsumeCheckpoint is the streaming node's consume checkpoint of the pchannel (WAL is alive if it moves).
	PChannelConsumeCheckpoint time.Time
	// Unflushed counts the segments on this vchannel that still hold data DataCoord has not
	// written out: Growing, Sealed and Flushing. Advancing a frozen checkpoint declares
	// everything before it flushed, so a vchannel with none of these has nothing left to lose.
	Unflushed int
	// Flushed counts the segments on this vchannel already written out.
	Flushed int
	// SeekPosition is the checkpoint's message ID rendered for the operator. DataCoord hands
	// this position to the flusher through GetChannelRecoveryInfo, and the flusher starts its
	// WAL scanner from the smallest one across the vchannels it serves: a position that has
	// aged out of the message queue's retention keeps the flusher from starting at all.
	SeekPosition string
	// Dropped is true when the checkpoint holds the dropped-channel sentinel
	// (funcutil.DroppedChannelCheckpointTimestamp), meaning the channel is gone, not stale.
	Dropped       bool
	DataCoordMark string // non-removed / removed / missing
	StreamingMeta string // NORMAL / DROPPED / missing
	Flags         []string
}

// Frozen reports whether the vchannel is flagged FROZEN.
func (v *VChannelHealth) Frozen() bool {
	for _, f := range v.Flags {
		if f == "FROZEN" {
			return true
		}
	}
	return false
}

// VChannelHealthCommand returns the `show vchannel-health` command.
func (c *ComponentShow) VChannelHealthCommand(ctx context.Context, p *VChannelHealthParam) (*framework.PresetResultSet, error) {
	// 1. DataCoord channel checkpoints, keyed by vchannel.
	cps, err := common.ListChannelCheckpoint(ctx, c.client, c.metaPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list channel checkpoints")
	}
	cpTS := make(map[string]uint64, len(cps))
	cpMsgID := make(map[string][]byte, len(cps))
	for _, cp := range cps {
		name := cp.GetProto().GetChannelName()
		cpTS[name] = cp.GetProto().GetTimestamp()
		cpMsgID[name] = cp.GetProto().GetMsgID()
	}

	// 2. Collections: vchannel -> collection, so the row can carry db.name and state.
	type collRef struct {
		id       int64
		name     string
		dbID     int64
		state    string
		pchannel string
	}
	vch2coll := make(map[string]collRef)
	collections, err := common.ListCollections(ctx, c.client, c.metaPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list collections")
	}
	for _, coll := range collections {
		info := coll.GetProto()
		if p.CollectionID != 0 && info.GetID() != p.CollectionID {
			continue
		}
		for _, ch := range coll.Channels() {
			vch2coll[ch.VirtualName] = collRef{
				id:       info.GetID(),
				name:     info.GetSchema().GetName(),
				dbID:     info.GetDbId(),
				state:    info.GetState().String(),
				pchannel: ch.PhysicalName,
			}
		}
	}
	dbNames := map[int64]string{}
	if dbs, err := common.ListDatabase(ctx, c.client, c.metaPath); err == nil {
		for _, db := range dbs {
			dbNames[db.GetProto().GetId()] = db.GetProto().GetName()
		}
	}

	// 3. DataCoord channel marks: "non-removed" = registered, "removed" = marked for removal.
	marks := map[string]string{}
	markPrefix := path.Join(c.metaPath, common.DCPrefix, common.ChannelRemovalPrefix) + "/"
	if keys, vals, err := c.client.LoadWithPrefix(ctx, markPrefix); err == nil {
		for i, k := range keys {
			marks[strings.TrimPrefix(k, markPrefix)] = vals[i]
		}
	}

	// 4. Segments per vchannel, split into "still holds unwritten data" and "already written".
	// This is what says whether advancing a frozen checkpoint would abandon anything: a
	// streaming node does not create segments for a vchannel missing from its recovery meta,
	// so a frozen vchannel with no unflushed segment has no pending data left to recover.
	unflushed := make(map[string]int)
	flushed := make(map[string]int)
	if segments, err := common.ListSegments(ctx, c.client, c.metaPath); err == nil {
		for _, seg := range segments {
			ch := seg.GetInsertChannel()
			if ch == "" {
				continue
			}
			switch seg.GetState() {
			case commonpb.SegmentState_Growing, commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing:
				unflushed[ch]++
			case commonpb.SegmentState_Flushed:
				flushed[ch]++
			}
		}
	}

	// 5. Streaming node recovery meta, per pchannel.
	pchannels := map[string]struct{}{}
	for vch := range cpTS {
		if ref, ok := vch2coll[vch]; ok && ref.pchannel != "" {
			pchannels[ref.pchannel] = struct{}{}
		} else if pch := physicalOf(vch); pch != "" {
			pchannels[pch] = struct{}{}
		}
	}
	streamState := map[string]string{}
	consumeCP := map[string]uint64{}
	for pch := range pchannels {
		rs, err := common.ListWALRecoveryStorage(ctx, c.client, c.metaPath, pch)
		if err != nil {
			continue // pchannel not managed by streaming service, or not found
		}
		if rs.Checkpoints != nil {
			consumeCP[pch] = rs.Checkpoints.GetTimeTick()
		}
		for _, vc := range rs.VChannels {
			streamState[vc.GetVchannel()] = strings.TrimPrefix(vc.GetState().String(), "VCHANNEL_STATE_")
		}
	}

	// 5. Analyze. The reference for "how far behind" is the pchannel's own consume checkpoint
	// (the WAL keeps moving while a dead vchannel does not); when the pchannel has none, fall
	// back to the newest checkpoint seen anywhere in the cluster, so a pchannel that carries a
	// single vchannel can still be judged.
	pchOf := func(vch string) string {
		if ref, ok := vch2coll[vch]; ok && ref.pchannel != "" {
			return ref.pchannel
		}
		return physicalOf(vch)
	}
	now := time.Now()
	var globalMax uint64
	for _, ts := range cpTS {
		if usableProgress(ts, now) && ts > globalMax {
			globalMax = ts
		}
	}
	for _, ts := range consumeCP {
		if usableProgress(ts, now) && ts > globalMax {
			globalMax = ts
		}
	}
	// The persisted consume checkpoint is written lazily, so it can trail the channel
	// checkpoints by minutes; the reference is therefore the newest progress known anywhere.
	refOf := func(pch string) uint64 {
		if cc, ok := consumeCP[pch]; ok && cc > globalMax && usableProgress(cc, now) {
			return cc
		}
		return globalMax
	}

	rows := make([]*VChannelHealth, 0, len(cpTS))
	for vch, ts := range cpTS {
		ref, known := vch2coll[vch]
		if p.CollectionID != 0 && (!known || ref.id != p.CollectionID) {
			continue
		}
		pch := pchOf(vch)
		dropped := ts == droppedChannelCheckpointTimestamp
		var lag float64
		if !dropped {
			lag = float64(int64(refOf(pch)>>18)-int64(ts>>18)) / 60000.0
			if lag < 0 {
				lag = 0
			}
		}
		row := &VChannelHealth{
			VChannel:      vch,
			Dropped:       dropped,
			PChannel:      pch,
			LagMinutes:    lag,
			Unflushed:     unflushed[vch],
			Flushed:       flushed[vch],
			SeekPosition:  formatSeekPosition(cpMsgID[vch]),
			DataCoordMark: markOrMissing(marks, vch),
			StreamingMeta: stateOrMissing(streamState, vch),
		}
		if !dropped {
			row.Checkpoint, _ = utils.ParseTS(ts)
		}
		if cc, ok := consumeCP[pch]; ok && usableProgress(cc, now) {
			row.PChannelConsumeCheckpoint, _ = utils.ParseTS(cc)
		}
		if known {
			db := dbNames[ref.dbID]
			if db == "" {
				db = fmt.Sprintf("db%d", ref.dbID)
			}
			row.Collection = fmt.Sprintf("%s.%s", db, ref.name)
			row.CollState = strings.TrimPrefix(ref.state, "Collection")
		} else {
			row.Collection = "(no collection meta)"
			row.Flags = append(row.Flags, "ORPHAN_CHECKPOINT")
		}
		if row.DataCoordMark == "removed" {
			row.Flags = append(row.Flags, "REMOVAL_MARK")
		} else if row.DataCoordMark == "missing" && known {
			row.Flags = append(row.Flags, "NO_DATACOORD_MARK")
		}
		if row.StreamingMeta == "missing" && known {
			row.Flags = append(row.Flags, "NOT_IN_STREAMING_META")
		} else if row.StreamingMeta != "NORMAL" && row.StreamingMeta != "missing" {
			row.Flags = append(row.Flags, "STREAMING_"+row.StreamingMeta)
		}
		if dropped {
			row.Flags = append(row.Flags, "DROPPED_CHECKPOINT")
		}
		if row.Unflushed > 0 {
			row.Flags = append(row.Flags, "UNFLUSHED_SEGMENTS")
		}
		if known && !dropped && row.DataCoordMark != "removed" && lag > float64(p.LagMinutes) {
			row.Flags = append(row.Flags, "FROZEN")
		}
		if !p.All && len(row.Flags) == 0 {
			continue
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Frozen() != rows[j].Frozen() {
			return rows[i].Frozen()
		}
		if rows[i].LagMinutes != rows[j].LagMinutes {
			return rows[i].LagMinutes > rows[j].LagMinutes
		}
		return rows[i].VChannel < rows[j].VChannel
	})

	return framework.NewPresetResultSet(framework.NewListResult[VChannelHealths](rows), framework.NameFormat(p.Format)), nil
}

func physicalOf(vchannel string) string {
	// <pchannel>_<collectionID>v<shard>
	idx := strings.LastIndex(vchannel, "_")
	if idx <= 0 {
		return ""
	}
	return vchannel[:idx]
}

// formatSeekPosition renders a checkpoint message ID. An 8-byte ID is a Kafka offset
// (kafka.SerializeKafkaID writes it little-endian), which is what an operator needs in order
// to compare it against the topic's earliest available offset; anything else is shown as hex.
func formatSeekPosition(msgID []byte) string {
	switch {
	case len(msgID) == 0:
		return "-"
	case len(msgID) == 8:
		return fmt.Sprintf("%d", int64(binary.LittleEndian.Uint64(msgID)))
	default:
		return hex.EncodeToString(msgID)
	}
}

func markOrMissing(marks map[string]string, vch string) string {
	if v, ok := marks[vch]; ok {
		return v
	}
	return "missing"
}

func stateOrMissing(states map[string]string, vch string) string {
	if v, ok := states[vch]; ok {
		return v
	}
	return "missing"
}

// VChannelHealths is the result set of `show vchannel-health`.
type VChannelHealths struct {
	framework.ListResultSet[*VChannelHealth]
}

func (rs *VChannelHealths) TableHeaders() table.Row {
	return table.Row{"VChannel", "Collection", "State", "Checkpoint (UTC)", "Lag(min)", "Unflushed", "Flushed", "Seek pos", "PCh consume-cp (UTC)", "DC mark", "Streaming meta", "Flags"}
}

func (rs *VChannelHealths) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(rs.Data))
	for _, r := range rs.Data {
		cp, lag := fmtTime(r.Checkpoint), fmt.Sprintf("%.0f", r.LagMinutes)
		if r.Dropped {
			cp, lag = "dropped", "-"
		}
		rows = append(rows, table.Row{
			r.VChannel, r.Collection, r.CollState, cp, lag, r.Unflushed, r.Flushed, r.SeekPosition,
			fmtTime(r.PChannelConsumeCheckpoint), r.DataCoordMark, r.StreamingMeta, strings.Join(r.Flags, ","),
		})
	}
	return rows
}

func (rs *VChannelHealths) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		frozen := 0
		for _, r := range rs.Data {
			if r.Frozen() {
				frozen++
			}
		}
		if frozen == 0 {
			fmt.Fprintf(sb, "no FROZEN vchannel (%d rows listed); all times are UTC\n", len(rs.Data))
		} else {
			fmt.Fprintf(sb, "%d FROZEN vchannel(s): GetFlushAllState stays false while any of them exists, so flushAll and a default-strategy milvus-backup create wait forever; all times are UTC\n", frozen)
		}
		for _, r := range rs.Data {
			cp, lag := fmtTime(r.Checkpoint), fmt.Sprintf("%.0fmin", r.LagMinutes)
			if r.Dropped {
				cp, lag = "dropped", "-"
			}
			fmt.Fprintf(sb, "%s  %s (%s)  checkpoint=%s  lag=%s  unflushed-segments=%d  flushed-segments=%d  seek-pos=%s  pchannel-consume-cp=%s  dc-mark=%s  streaming-meta=%s  %s\n",
				r.VChannel, r.Collection, r.CollState, cp, lag, r.Unflushed, r.Flushed, r.SeekPosition,
				fmtTime(r.PChannelConsumeCheckpoint), r.DataCoordMark, r.StreamingMeta, strings.Join(r.Flags, ","))
		}
		return sb.String()
	case framework.FormatJSON:
		return framework.MarshalJSON(rs.Data)
	default:
	}
	return ""
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format("2006-01-02 15:04:05")
}
