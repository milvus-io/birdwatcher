package reset

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type ResetCheckpointParam struct {
	framework.ExecutionParam `use:"reset checkpoint" desc:"reset every persisted MQ position to the target WAL's earliest position, for switching a stopped instance onto a different MQ. MILVUS MUST BE STOPPED; on failure re-run until it succeeds before starting it."`
	TargetWAL                string `name:"target-wal" default:"" desc:"target WAL type: woodpecker, pulsar, kafka, rocksmq"`
	PChannel                 string `name:"pchannel" default:"" desc:"restrict to a single pchannel; empty means every pchannel"`
	AllowGrowing             bool   `name:"allow-growing" default:"false" desc:"proceed even when growing segments exist; DANGEROUS, their unflushed data is lost"`
}

// write kinds, ordered so that the consume checkpoint lands last.
const (
	kindCollection = "collection"
	kindSegment    = "segment"
	kindChannelCP  = "channel-cp"
	kindConsumeCP  = "consume-checkpoint"
)

// writeOrder decides the apply sequence.
//
// There is NO safe intermediate state, in any order. The consume checkpoint
// decides which WAL streamingnode opens; the channel checkpoints decide which
// positions it then decodes. The moment those two disagree, milvus panics on
// startup — verified: the flusher blows up in getRecoveryInfos ->
// MustGetMessageIDFromMQWrapperIDBytesWithWALName with "proto: cannot parse
// invalid wire-format data". Reordering only flips which side is stale.
//
// So the guarantee this command offers is not "safe to start after a partial
// run" but "safe to re-run": every write is idempotent and a repeat run
// rewrites exactly the keys the previous one missed. Operators must re-run
// until it reports success before starting Milvus.
var writeOrder = []string{kindCollection, kindSegment, kindChannelCP, kindConsumeCP}

type plannedWrite struct {
	kind   string
	key    string
	value  []byte
	before string
	after  string
}

// ResetCheckpointCommand implements `reset checkpoint`.
func (c *ComponentReset) ResetCheckpointCommand(ctx context.Context, p *ResetCheckpointParam) error {
	pos, err := buildWALPosition(p.TargetWAL)
	if err != nil {
		return err
	}

	if err := c.preflight(ctx, p); err != nil {
		return err
	}

	writes, err := c.plan(ctx, p, pos)
	if err != nil {
		return err
	}
	deletes, err := c.planCleanup(ctx, p)
	if err != nil {
		return err
	}

	if detail, split := c.detectSplitState(ctx, p); split {
		fmt.Printf("!! previous run did not finish: %s\n", detail)
		fmt.Printf("!! Milvus cannot start in this state; this run will finish the switch.\n\n")
	}

	printPlan(writes, deletes, pos, p.Run)
	if !p.Run {
		return nil
	}
	if err := c.apply(ctx, writes); err != nil {
		return err
	}
	return c.applyCleanup(ctx, deletes)
}

// inScope reports whether a vchannel/pchannel name is covered by --pchannel.
func (c *ComponentReset) inScope(p *ResetCheckpointParam, channel string) bool {
	if p.PChannel == "" {
		return true
	}
	return funcutil.ToPhysicalChannel(channel) == p.PChannel
}

func (c *ComponentReset) plan(ctx context.Context, p *ResetCheckpointParam, pos *walPosition) ([]plannedWrite, error) {
	var writes []plannedWrite

	collWrites, err := c.planCollections(ctx, p, pos)
	if err != nil {
		return nil, err
	}
	writes = append(writes, collWrites...)

	segWrites, err := c.planSegments(ctx, p, pos)
	if err != nil {
		return nil, err
	}
	writes = append(writes, segWrites...)

	cpWrites, err := c.planChannelCheckpoints(ctx, p, pos)
	if err != nil {
		return nil, err
	}
	writes = append(writes, cpWrites...)

	consumeWrites, err := c.planConsumeCheckpoints(ctx, p, pos)
	if err != nil {
		return nil, err
	}
	writes = append(writes, consumeWrites...)

	return writes, nil
}

// planCollections rewrites CollectionInfo.StartPositions, the last level of the
// three-level seek fallback in datacoord (channel-cp -> segment pos -> here).
func (c *ComponentReset) planCollections(ctx context.Context, p *ResetCheckpointParam, pos *walPosition) ([]plannedWrite, error) {
	colls, err := common.ListCollections(ctx, c.client, c.basePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list collections")
	}

	var writes []plannedWrite
	for _, coll := range colls {
		pb := coll.GetProto()
		var touched []*commonpb.KeyDataPair
		changed := false
		for _, sp := range pb.GetStartPositions() {
			if !c.inScope(p, sp.GetKey()) || bytes.Equal(sp.GetData(), pos.raw) {
				touched = append(touched, sp)
				continue
			}
			touched = append(touched, &commonpb.KeyDataPair{Key: sp.GetKey(), Data: pos.raw})
			changed = true
		}
		if !changed {
			continue
		}

		next := proto.Clone(pb).(*etcdpb.CollectionInfo)
		next.StartPositions = touched
		value, err := proto.Marshal(next)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to marshal collection %d", pb.GetID())
		}
		writes = append(writes, plannedWrite{
			kind:   kindCollection,
			key:    coll.Key(),
			value:  value,
			before: fmt.Sprintf("collection %d startPositions=%d", pb.GetID(), len(pb.GetStartPositions())),
			after:  pos.String(),
		})
	}
	return writes, nil
}

// planSegments rewrites SegmentInfo.StartPosition and DmlPosition, the middle
// level of the seek fallback.
func (c *ComponentReset) planSegments(ctx context.Context, p *ResetCheckpointParam, pos *walPosition) ([]plannedWrite, error) {
	segments, err := common.ListSegments(ctx, c.client, c.basePath, func(s *models.Segment) bool {
		return c.inScope(p, s.InsertChannel)
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list segments")
	}

	var writes []plannedWrite
	for _, seg := range segments {
		pb := seg.SegmentInfo
		if pb.GetStartPosition() == nil && pb.GetDmlPosition() == nil {
			continue
		}

		if alreadyAtOrNil(pb.GetStartPosition(), pos) && alreadyAtOrNil(pb.GetDmlPosition(), pos) {
			continue
		}

		next := proto.Clone(pb).(*datapb.SegmentInfo)
		before := fmt.Sprintf("segment %d start=%s dml=%s", pb.GetID(),
			describePosition(pb.GetStartPosition()), describePosition(pb.GetDmlPosition()))
		rewritePosition(next.GetStartPosition(), pos)
		rewritePosition(next.GetDmlPosition(), pos)

		value, err := proto.Marshal(next)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to marshal segment %d", pb.GetID())
		}
		writes = append(writes, plannedWrite{
			kind:   kindSegment,
			key:    seg.GetKey(),
			value:  value,
			before: before,
			after:  pos.String(),
		})
	}
	return writes, nil
}

// planChannelCheckpoints rewrites datacoord-meta/channel-cp/<vchannel>, the
// first level of the seek fallback and the one datacoord actually hands out.
func (c *ComponentReset) planChannelCheckpoints(ctx context.Context, p *ResetCheckpointParam, pos *walPosition) ([]plannedWrite, error) {
	cps, err := common.ListChannelCheckpoint(ctx, c.client, c.basePath, func(mp *models.MsgPosition) bool {
		return c.inScope(p, mp.GetProto().GetChannelName())
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list channel checkpoints")
	}

	var writes []plannedWrite
	for _, cp := range cps {
		pb := cp.GetProto()
		if alreadyAt(pb, pos) {
			continue
		}
		next := proto.Clone(pb).(*msgpb.MsgPosition)
		before := fmt.Sprintf("vchannel %s %s", pb.GetChannelName(), describePosition(pb))
		rewritePosition(next, pos)

		value, err := proto.Marshal(next)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to marshal checkpoint for %s", pb.GetChannelName())
		}
		writes = append(writes, plannedWrite{
			kind:   kindChannelCP,
			key:    cp.Key(),
			value:  value,
			before: before,
			after:  pos.String(),
		})
	}
	return writes, nil
}

// planConsumeCheckpoints rewrites streamingnode's per-pchannel consume
// checkpoint. streamingnode reads its WALName to decide which WAL to open, so
// this is the write that actually performs the switch.
func (c *ComponentReset) planConsumeCheckpoints(ctx context.Context, p *ResetCheckpointParam, pos *walPosition) ([]plannedWrite, error) {
	channels, err := common.ListWALDistribution(ctx, c.client, c.basePath, p.PChannel)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list wal distribution")
	}

	var writes []plannedWrite
	for _, ch := range channels {
		pchannel := ch.GetChannel().GetName()
		meta, err := common.ListWALRecoveryStorage(ctx, c.client, c.basePath, pchannel)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read recovery storage of %s", pchannel)
		}
		if meta.Checkpoints == nil {
			fmt.Printf("pchannel %s has no consume checkpoint yet, skipping\n", pchannel)
			continue
		}

		if cur := meta.Checkpoints.GetMessageId(); cur.GetWALName() == pos.walName &&
			cur.GetId() == pos.msgID.Id && meta.Checkpoints.GetAlterWalState() == nil {
			continue
		}

		next := proto.Clone(meta.Checkpoints).(*streamingpb.WALCheckpoint)
		before := fmt.Sprintf("pchannel %s %s@%d", pchannel,
			common.GetMessageIDString("", meta.Checkpoints.GetMessageId().GetId()),
			meta.Checkpoints.GetTimeTick())

		// Only the message id moves. TimeTick is a global TSO and stays valid
		// across MQ implementations; AlterWalState is cleared so streamingnode
		// does not try to resume an interrupted online switch.
		//
		// ReplicateCheckpoint is left alone: it is a subscription position against
		// a remote cluster, owned and maintained by Milvus itself, and switching
		// the local WAL says nothing about it.
		next.MessageId = pos.msgID
		next.AlterWalState = nil

		value, err := proto.Marshal(next)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to marshal consume checkpoint of %s", pchannel)
		}
		writes = append(writes, plannedWrite{
			kind:   kindConsumeCP,
			key:    common.ConsumeCheckpointKey(c.basePath, pchannel),
			value:  value,
			before: before,
			after:  pos.String(),
		})
	}
	return writes, nil
}

// alreadyAt reports whether a position already points at the target, so a
// repeated run can show "nothing to do" instead of listing no-op rewrites.
// bytes.Equal treats nil and empty alike, which matters for woodpecker's
// earliest id — it serializes to zero bytes and reads back as nil.
func alreadyAt(mp *msgpb.MsgPosition, pos *walPosition) bool {
	return mp != nil && mp.GetWALName() == pos.walName && bytes.Equal(mp.GetMsgID(), pos.raw)
}

// rewritePosition points a MsgPosition at the target WAL. Timestamp is kept: it
// is a global TSO, unrelated to which MQ stores the message.
func rewritePosition(mp *msgpb.MsgPosition, pos *walPosition) {
	if mp == nil {
		return
	}
	mp.MsgID = pos.raw
	mp.WALName = pos.walName
}

// alreadyAtOrNil treats an absent position as "nothing to do": reset only
// rewrites positions that exist.
func alreadyAtOrNil(mp *msgpb.MsgPosition, pos *walPosition) bool {
	return mp == nil || alreadyAt(mp, pos)
}

func describePosition(mp *msgpb.MsgPosition) string {
	if mp == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s@%d", mp.GetWALName().String(), mp.GetTimestamp())
}

// detectSplitState reports the WAL names currently recorded on each side of the
// switch. A previous run that died partway leaves them disagreeing, which is
// exactly the state that crashes Milvus on startup — worth naming explicitly so
// the operator knows they are finishing a job rather than starting one.
func (c *ComponentReset) detectSplitState(ctx context.Context, p *ResetCheckpointParam) (string, bool) {
	seen := map[string]map[commonpb.WALName]int{
		kindChannelCP: {},
		kindConsumeCP: {},
	}

	cps, err := common.ListChannelCheckpoint(ctx, c.client, c.basePath, func(mp *models.MsgPosition) bool {
		return c.inScope(p, mp.GetProto().GetChannelName())
	})
	if err != nil {
		return "", false
	}
	for _, cp := range cps {
		seen[kindChannelCP][cp.GetProto().GetWALName()]++
	}

	channels, err := common.ListWALDistribution(ctx, c.client, c.basePath, p.PChannel)
	if err != nil {
		return "", false
	}
	for _, ch := range channels {
		meta, err := common.ListWALRecoveryStorage(ctx, c.client, c.basePath, ch.GetChannel().GetName())
		if err != nil || meta.Checkpoints == nil {
			continue
		}
		seen[kindConsumeCP][meta.Checkpoints.GetMessageId().GetWALName()]++
	}

	// Compare the SET of WAL names, never the counts: the two sides are indexed
	// differently (one key per vchannel vs one per pchannel), so their counts
	// legitimately differ even when the instance is perfectly consistent.
	names := func(m map[commonpb.WALName]int) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k.String())
		}
		sort.Strings(out)
		return out
	}
	chNames, cpNames := names(seen[kindChannelCP]), names(seen[kindConsumeCP])
	if len(chNames) == 0 || len(cpNames) == 0 {
		return "", false
	}
	if strings.Join(chNames, ",") == strings.Join(cpNames, ",") {
		return "", false
	}
	return fmt.Sprintf("channel-cp says [%s] but consume-checkpoint says [%s]",
		strings.Join(chNames, ", "), strings.Join(cpNames, ", ")), true
}

func printPlan(writes []plannedWrite, deletes []plannedDelete, pos *walPosition, run bool) {
	byKind := map[string]int{}
	for _, w := range writes {
		byKind[w.kind]++
	}

	fmt.Printf("=== reset checkpoint -> %s ===\n", pos)
	if len(writes) == 0 && len(deletes) == 0 {
		fmt.Println("  every position already points at this WAL; nothing to do.")
		return
	}
	for _, kind := range writeOrder {
		fmt.Printf("  %-20s %d key(s)\n", kind, byKind[kind])
	}
	for _, kind := range deleteOrder {
		if n := countDeleteKind(deletes, kind); n > 0 {
			fmt.Printf("  %-20s %d prefix(es) to DELETE\n", kind, n)
		}
	}
	fmt.Println()
	for _, kind := range writeOrder {
		for _, w := range writes {
			if w.kind != kind {
				continue
			}
			fmt.Printf("  [%s] %s\n      %s  ->  %s\n", w.kind, w.key, w.before, w.after)
		}
	}
	for _, kind := range deleteOrder {
		for _, d := range deletes {
			if d.kind != kind {
				continue
			}
			fmt.Printf("  [DELETE %s] %s\n      %s\n", d.kind, d.key, d.reason)
		}
	}
	fmt.Printf("\nNot touched: streamingcoord pchannel assignment and channelwatch info —\n")
	fmt.Printf("they carry no MQ position and Milvus rebuilds them on startup.\n")
	if !run {
		fmt.Printf("\ndry run: nothing written. Re-run with --run=true to apply.\n")
		fmt.Printf("Make sure Milvus is STOPPED and you have a `backup` before applying.\n")
	}
}

func (c *ComponentReset) apply(ctx context.Context, writes []plannedWrite) error {
	for _, kind := range writeOrder {
		for _, w := range writes {
			if w.kind != kind {
				continue
			}
			if err := c.client.Save(ctx, w.key, string(w.value)); err != nil {
				return errors.Wrapf(err, "failed to write %s (%s).\n"+
					"METADATA IS HALF-RESET AND MILVUS WILL CRASH IF STARTED NOW: streamingnode "+
					"panics decoding a position written for a different WAL.\n"+
					"Re-run this command until it reports success, then start Milvus", w.key, w.kind)
			}
		}
		if n := countKind(writes, kind); n > 0 {
			fmt.Printf("applied %d %s key(s)\n", n, kind)
		}
	}
	fmt.Println("reset done. Update mq.type in the Milvus config before starting it back up.")
	return nil
}

func countKind(writes []plannedWrite, kind string) int {
	n := 0
	for _, w := range writes {
		if w.kind == kind {
			n++
		}
	}
	return n
}

func countDeleteKind(deletes []plannedDelete, kind string) int {
	n := 0
	for _, d := range deletes {
		if d.kind == kind {
			n++
		}
	}
	return n
}
