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
// behind. None of them carry data: they are allocations, tombstones and caches
// that Milvus rebuilds on startup.
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
	kindChannelRemoval  = "channel-removal"
	kindQueryCoordCache = "querycoord-target-cache"
)

var deleteOrder = []string{kindSegmentAssign, kindChannelRemoval, kindQueryCoordCache}

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

	removals, err := c.planChannelRemovals(ctx, p)
	if err != nil {
		return nil, err
	}
	deletes = append(deletes, removals...)

	targets, err := c.planQueryCoordTargets(ctx, p)
	if err != nil {
		return nil, err
	}
	deletes = append(deletes, targets...)

	return deletes, nil
}

// planChannelRemovals drops the removal markers of the channels in scope. The
// markers are keyed by channel name, so --pchannel narrows them exactly.
func (c *ComponentReset) planChannelRemovals(ctx context.Context, p *ResetCheckpointParam) ([]plannedDelete, error) {
	prefix := path.Join(c.basePath, common.DCPrefix, common.ChannelRemovalPrefix)
	keys, _, err := c.client.LoadWithPrefix(ctx, prefix+"/", kv.WithKeysOnly())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to scan %s", prefix)
	}

	var deletes []plannedDelete
	for _, key := range keys {
		if !c.inScope(p, path.Base(key)) {
			continue
		}
		deletes = append(deletes, plannedDelete{
			kind:   kindChannelRemoval,
			key:    key,
			reason: "stale channel removal marker would block re-watch",
		})
	}
	return deletes, nil
}

// planQueryCoordTargets drops the cached query target of every collection with
// at least one vchannel in scope.
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
	colls, err := common.ListCollections(ctx, c.client, c.basePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list collections")
	}

	var deletes []plannedDelete
	for _, coll := range colls {
		id := coll.GetProto().GetID()
		key := path.Join(c.basePath, collectionTargetPrefix, strconv.FormatInt(id, 10))
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
