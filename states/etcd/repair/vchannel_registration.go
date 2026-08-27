package repair

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// VChannelRegistrationParam is the parameter of `repair vchannel-registration`.
type VChannelRegistrationParam struct {
	framework.ExecutionParam `use:"repair vchannel-registration" desc:"restore vchannels that DataCoord still tracks but the streaming node's recovery metadata has lost, which freezes their flush checkpoints and keeps GetFlushAllState false forever"`
	VChannel                 string `name:"vchannel" default:"" desc:"restore only this vchannel, empty for every candidate"`
}

// vchannelRepair is one planned restoration.
type vchannelRepair struct {
	vchannel   string
	pchannel   string
	collection *models.Collection
	// curCP is the frozen checkpoint DataCoord holds for this vchannel.
	curCP *msgpb.MsgPosition
	// liveCP is a checkpoint of a healthy vchannel on the same pchannel. The flusher takes
	// its seek position from the channel checkpoint (datacoord GetChannelSeekPosition returns
	// it first) and starts from the smallest one across the vchannels it serves, so a frozen
	// position that has aged out of the message queue's retention would keep the flusher for
	// the whole pchannel from starting. Moving the checkpoint here first avoids that.
	liveCP *msgpb.MsgPosition
	// needsCPMove is false when the checkpoint is already at or past the live position, in
	// which case only the recovery metadata is missing. Moving it then would drag the
	// checkpoint backwards and make the flusher re-consume.
	needsCPMove bool
	// unflushed is the number of segments on this vchannel still holding unwritten data.
	// Advancing the checkpoint declares everything before it flushed, so this must be zero.
	unflushed int
}

// VChannelRegistrationCommand returns the `repair vchannel-registration` command.
func (c *ComponentRepair) VChannelRegistrationCommand(ctx context.Context, p *VChannelRegistrationParam) error {
	plans, err := c.planVChannelRepairs(ctx, p.VChannel)
	if err != nil {
		return err
	}
	if len(plans) == 0 {
		fmt.Println("no vchannel needs restoring: every checkpoint DataCoord tracks is present in the streaming node's recovery metadata")
		return nil
	}

	canFix := func(r *vchannelRepair) bool {
		if r.unflushed > 0 {
			return false
		}
		// A stale checkpoint can only be repaired if there is a live position to move it to.
		// One that is already current needs no move, so a missing sibling does not block it.
		return !r.needsCPMove || r.liveCP != nil
	}
	blocked := lo.Filter(plans, func(r *vchannelRepair, _ int) bool { return !canFix(r) })
	ready := lo.Filter(plans, func(r *vchannelRepair, _ int) bool { return canFix(r) })

	fmt.Printf("%d vchannel(s) missing from the streaming node's recovery metadata\n\n", len(plans))
	for _, r := range plans {
		curT, _ := utils.ParseTS(r.curCP.GetTimestamp())
		line := fmt.Sprintf("  %s  %s.%s\n      checkpoint %s  unflushed-segments=%d",
			r.vchannel, dbLabel(r.collection), r.collection.GetProto().GetSchema().GetName(),
			curT.UTC().Format("2006-01-02 15:04:05"), r.unflushed)
		if r.needsCPMove {
			liveT, _ := utils.ParseTS(r.liveCP.GetTimestamp())
			line += fmt.Sprintf("\n      will move checkpoint to %s (from %s on the same pchannel)",
				liveT.UTC().Format("2006-01-02 15:04:05"), r.liveCP.GetChannelName())
		} else {
			line += "\n      checkpoint already current, only the recovery metadata is missing"
		}
		if r.unflushed > 0 {
			line += "\n      SKIPPED: has unflushed segments, moving its checkpoint would abandon them"
		} else if r.needsCPMove && r.liveCP == nil {
			line += "\n      SKIPPED: no healthy vchannel on this pchannel to take a live checkpoint from"
		}
		fmt.Println(line)
	}
	fmt.Println()

	if len(blocked) > 0 {
		fmt.Printf("%d skipped, %d can be restored\n", len(blocked), len(ready))
	}
	if len(ready) == 0 {
		return nil
	}

	if !p.Run {
		fmt.Println("dry run, nothing written. re-run with --run to apply.")
		fmt.Println("note: the streaming nodes serving these pchannels must be restarted afterwards,")
		fmt.Println("      the recovery metadata is only read when the WAL is opened.")
		return nil
	}

	for _, r := range ready {
		if err := c.applyVChannelRepair(ctx, r); err != nil {
			return errors.Wrapf(err, "restore vchannel %s", r.vchannel)
		}
		fmt.Printf("%s restored\n", r.vchannel)
	}
	fmt.Println()
	fmt.Println("restart the streaming nodes serving these pchannels to pick the entries up.")
	return nil
}

// applyVChannelRepair moves the checkpoint first, then writes the recovery metadata. The
// order matters: registering a vchannel whose checkpoint still points at a position the
// message queue has dropped keeps the flusher for the entire pchannel from starting.
func (c *ComponentRepair) applyVChannelRepair(ctx context.Context, r *vchannelRepair) error {
	if err := c.moveCheckpoint(ctx, r); err != nil {
		return err
	}
	return c.writeRecoveryMeta(ctx, r)
}

// moveCheckpoint advances the vchannel's channel checkpoint to a live position, and does
// nothing when it is already there: writing an older position would drag the flusher back.
func (c *ComponentRepair) moveCheckpoint(ctx context.Context, r *vchannelRepair) error {
	if !r.needsCPMove {
		return nil
	}
	cp := &msgpb.MsgPosition{
		ChannelName: r.vchannel,
		MsgID:       r.liveCP.GetMsgID(),
		MsgGroup:    r.liveCP.GetMsgGroup(),
		Timestamp:   r.liveCP.GetTimestamp(),
	}
	bs, err := proto.Marshal(cp)
	if err != nil {
		return errors.Wrap(err, "marshal channel checkpoint")
	}
	key := path.Join(c.basePath, common.DCPrefix, "channel-cp", r.vchannel)
	if err := c.client.Save(ctx, key, string(bs)); err != nil {
		return errors.Wrap(err, "save channel checkpoint")
	}
	return nil
}

// writeRecoveryMeta puts the vchannel back into the streaming node's recovery metadata,
// rebuilt from what rootcoord still holds for the collection.
func (c *ComponentRepair) writeRecoveryMeta(ctx context.Context, r *vchannelRepair) error {
	info := r.collection.GetProto()
	partitions, err := common.ListCollectionPartitions(ctx, c.client, c.basePath, info.GetID())
	if err != nil {
		return errors.Wrap(err, "list partitions")
	}

	meta := &streamingpb.VChannelMeta{
		Vchannel: r.vchannel,
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: info.GetID(),
			Partitions: lo.Map(partitions, func(p *models.Partition, _ int) *streamingpb.PartitionInfoOfVChannel {
				return &streamingpb.PartitionInfoOfVChannel{PartitionId: p.GetProto().GetPartitionID()}
			}),
		},
		// Zero, the same value initializeRecoverInfo seeds: there is no schema version from
		// before the vchannel was restored, so every message is treated as arriving after it.
		CheckpointTimeTick: 0,
	}
	if err := common.SaveVChannelMeta(ctx, c.client, c.basePath, meta); err != nil {
		return err
	}

	schema := &streamingpb.CollectionSchemaOfVChannel{
		Schema: &schemapb.CollectionSchema{
			Name:               info.GetSchema().GetName(),
			Description:        info.GetSchema().GetDescription(),
			AutoID:             info.GetSchema().GetAutoID(),
			Fields:             info.GetSchema().GetFields(),
			EnableDynamicField: info.GetSchema().GetEnableDynamicField(),
			Properties:         info.GetProperties(),
			Functions: lo.Map(r.collection.Functions, func(f *models.Function, _ int) *schemapb.FunctionSchema {
				return f.GetProto()
			}),
			DbName:            info.GetSchema().GetDbName(),
			StructArrayFields: info.GetSchema().GetStructArrayFields(),
		},
		CheckpointTimeTick: 0,
		State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
	}
	return common.SaveSchemaForVChannel(ctx, c.client, c.basePath, meta, schema)
}

// dbLabel names the collection's database for display: the schema carries the name once
// it has been set, otherwise fall back to the numeric id the collection meta holds.
func dbLabel(coll *models.Collection) string {
	if name := coll.GetProto().GetSchema().GetDbName(); name != "" {
		return name
	}
	return fmt.Sprintf("db%d", coll.GetProto().GetDbId())
}

// planVChannelRepairs finds every vchannel DataCoord still tracks that the streaming node's
// recovery metadata has no record of, and works out what each one needs.
func (c *ComponentRepair) planVChannelRepairs(ctx context.Context, only string) ([]*vchannelRepair, error) {
	cps, err := common.ListChannelCheckpoint(ctx, c.client, c.basePath)
	if err != nil {
		return nil, errors.Wrap(err, "list channel checkpoints")
	}
	cpOf := make(map[string]*msgpb.MsgPosition, len(cps))
	for _, cp := range cps {
		cpOf[cp.GetProto().GetChannelName()] = cp.GetProto()
	}

	collections, err := common.ListCollections(ctx, c.client, c.basePath)
	if err != nil {
		return nil, errors.Wrap(err, "list collections")
	}
	collOf := make(map[string]*models.Collection)
	pchOf := make(map[string]string)
	for _, coll := range collections {
		for _, ch := range coll.Channels() {
			collOf[ch.VirtualName] = coll
			pchOf[ch.VirtualName] = ch.PhysicalName
		}
	}

	marks := map[string]string{}
	markPrefix := path.Join(c.basePath, common.DCPrefix, common.ChannelRemovalPrefix) + "/"
	if keys, vals, err := c.client.LoadWithPrefix(ctx, markPrefix); err == nil {
		for i, k := range keys {
			marks[strings.TrimPrefix(k, markPrefix)] = vals[i]
		}
	}

	known := map[string]struct{}{}
	for pch := range lo.SliceToMap(lo.Values(pchOf), func(p string) (string, struct{}) { return p, struct{}{} }) {
		rs, err := common.ListWALRecoveryStorage(ctx, c.client, c.basePath, pch)
		if err != nil {
			continue
		}
		for _, vc := range rs.VChannels {
			known[vc.GetVchannel()] = struct{}{}
		}
	}

	unflushed := map[string]int{}
	if segments, err := common.ListSegments(ctx, c.client, c.basePath); err == nil {
		for _, seg := range segments {
			switch seg.GetState() {
			case commonpb.SegmentState_Growing, commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing:
				unflushed[seg.GetInsertChannel()]++
			}
		}
	}

	// A live checkpoint per pchannel: the newest checkpoint among the vchannels the
	// streaming node still serves there.
	liveOf := map[string]*msgpb.MsgPosition{}
	for vch, cp := range cpOf {
		if _, ok := known[vch]; !ok {
			continue
		}
		pch := pchOf[vch]
		if pch == "" {
			continue
		}
		if cur, ok := liveOf[pch]; !ok || cp.GetTimestamp() > cur.GetTimestamp() {
			liveOf[pch] = cp
		}
	}

	var out []*vchannelRepair
	for vch, cp := range cpOf {
		if only != "" && vch != only {
			continue
		}
		if _, ok := known[vch]; ok {
			continue // the streaming node has it, nothing to restore
		}
		coll, ok := collOf[vch]
		if !ok {
			continue // no collection behind it, not ours to restore
		}
		if marks[vch] == "removed" {
			continue // DataCoord marked it for removal, it is meant to be gone
		}
		live := liveOf[pchOf[vch]]
		out = append(out, &vchannelRepair{
			vchannel:    vch,
			pchannel:    pchOf[vch],
			collection:  coll,
			curCP:       cp,
			liveCP:      live,
			needsCPMove: live != nil && live.GetTimestamp() > cp.GetTimestamp(),
			unflushed:   unflushed[vch],
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].vchannel < out[j].vchannel })
	return out, nil
}
