package reset

import (
	"context"
	"fmt"
	"path"
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// Prefixes holding state that is only meaningful against the WAL we are leaving
// behind. Neither carries data: one is a set of growing-segment allocations that
// can no longer be fed, the other a cache of positions Milvus rebuilds.
//
// datacoord's channel-removal prefix looks like a third candidate and is not.
// Despite the name it is a two-valued flag, and MarkChannelDeleted has no callers
// today — so every key under it is the NonRemoveFlagTomestone that MarkChannelAdded
// writes once at collection creation. Nothing rebuilds it, and datacoord reads it
// (ChannelExists) to decide whether to hold back GC of dropped segments whose DML
// position is ahead of the channel checkpoint. Deleting it would silently and
// permanently disable that guard.
const (
	collectionTargetPrefix = "queryCoord-Collection-Target"
)

// plannedDelete is a key or subtree removed as part of the switch.
type plannedDelete struct {
	kind       string
	key        string
	withPrefix bool
	reason     string
}

const (
	kindSegmentAssign   = "segment-assign"
	kindQueryCoordCache = "querycoord-target-cache"
)

var deleteOrder = []string{kindSegmentAssign, kindQueryCoordCache}

// planCleanup lists the adjacent state to drop. Growing segments are already
// ruled out by preflight, so every segment-assign record left here is stale.
func (c *ComponentReset) planCleanup(ctx context.Context, p *ResetCheckpointParam) ([]plannedDelete, error) {
	var deletes []plannedDelete

	channels, err := common.ListWALDistribution(ctx, c.client, c.basePath, p.PChannel)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list wal distribution")
	}
	for _, ch := range channels {
		pchannel := ch.GetChannel().GetName()
		meta, err := common.ListWALRecoveryStorage(ctx, c.client, c.basePath, pchannel)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read recovery storage of %s", pchannel)
		}
		n := 0
		for _, segs := range meta.Segments {
			n += len(segs)
		}
		for _, segs := range meta.RedundantSegments {
			n += len(segs)
		}
		if n == 0 {
			continue
		}
		deletes = append(deletes, plannedDelete{
			kind:       kindSegmentAssign,
			key:        common.SegmentAssignPrefix(c.basePath, pchannel),
			withPrefix: true,
			reason:     fmt.Sprintf("%d stale growing-segment allocation(s) pinned to the old WAL", n),
		})
	}

	targets, err := c.planQueryCoordTargets(ctx, p)
	if err != nil {
		return nil, err
	}
	deletes = append(deletes, targets...)

	return deletes, nil
}

// planQueryCoordTargets drops cached query targets: the whole prefix on a
// whole-instance run, otherwise every collection with at least one vchannel in
// scope.
//
// querycoord recovers this cache into its in-memory current target and hands the
// seek positions inside it straight to querynodes (TargetManager.Recover ->
// task executor's req.Checkpoint). Those positions carry the old WAL's encoding
// and decode cleanly, so nothing detects them as stale — a rewound instance would
// seek the new MQ with old-WAL positions. Dropping the cache costs only a rebuild
// from datacoord, which is what an ordinary restart does anyway.
//
// A collection sharded across several pchannels keeps positions for all of them
// in one target, so touching any one of its vchannels dirties the whole entry.
func (c *ComponentReset) planQueryCoordTargets(ctx context.Context, p *ResetCheckpointParam) ([]plannedDelete, error) {
	// A whole-instance run drops the entire prefix. Enumerating collections
	// instead would miss orphan targets — a collection whose meta is already
	// gone leaves its cached target behind, and ListCollections cannot see it.
	prefix := path.Join(c.basePath, collectionTargetPrefix)
	if p.PChannel == "" {
		keys, _, err := c.client.LoadWithPrefix(ctx, prefix+"/", kv.WithKeysOnly())
		if err != nil {
			return nil, errors.Wrapf(err, "failed to scan %s", prefix)
		}
		if len(keys) == 0 {
			return nil, nil
		}
		return []plannedDelete{{
			kind:       kindQueryCoordCache,
			key:        prefix,
			withPrefix: true,
			reason: fmt.Sprintf("%d cached query target(s) embed old-WAL positions; querycoord rebuilds them",
				len(keys)),
		}}, nil
	}

	colls, err := common.ListCollections(ctx, c.client, c.basePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list collections")
	}

	var deletes []plannedDelete
	for _, coll := range colls {
		id := coll.GetProto().GetID()
		key := path.Join(prefix, strconv.FormatInt(id, 10))
		if !c.anyChannelInScope(p, coll.GetProto().GetVirtualChannelNames()) {
			continue
		}
		// a single key, not a subtree
		exists, err := c.keyExists(ctx, key)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		deletes = append(deletes, plannedDelete{
			kind:   kindQueryCoordCache,
			key:    key,
			reason: fmt.Sprintf("collection %d: cached query target embeds old-WAL positions; querycoord rebuilds it", id),
		})
	}
	return deletes, nil
}

// keyExists reports whether a single key is present, so the plan only lists
// deletions that actually remove something.
func (c *ComponentReset) keyExists(ctx context.Context, key string) (bool, error) {
	_, err := c.client.Load(ctx, key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to read %s", key)
	}
	return true, nil
}

// anyChannelInScope reports whether any of the collection's vchannels is covered
// by --pchannel.
func (c *ComponentReset) anyChannelInScope(p *ResetCheckpointParam, vchannels []string) bool {
	for _, vchannel := range vchannels {
		if c.inScope(p, vchannel) {
			return true
		}
	}
	return false
}

func (c *ComponentReset) applyCleanup(ctx context.Context, deletes []plannedDelete) error {
	for _, kind := range deleteOrder {
		for _, d := range deletes {
			if d.kind != kind {
				continue
			}
			var err error
			if d.withPrefix {
				err = c.client.RemoveWithPrefix(ctx, d.key)
			} else {
				err = c.client.Remove(ctx, d.key)
			}
			if err != nil {
				return errors.Wrapf(err, "failed to delete %s (%s)", d.key, d.kind)
			}
			fmt.Printf("deleted %s (%s)\n", d.key, d.kind)
		}
	}
	return nil
}
