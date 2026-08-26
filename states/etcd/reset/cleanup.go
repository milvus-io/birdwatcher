package reset

import (
	"context"
	"fmt"
	"path"

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

	// Only whole-instance runs may drop these: they are not partitioned by
	// pchannel, so removing them during a single-pchannel run would disturb
	// channels the operator did not ask us to touch.
	if p.PChannel == "" {
		candidates := []plannedDelete{
			{
				kind:       kindChannelRemoval,
				key:        path.Join(c.basePath, common.DCPrefix, common.ChannelRemovalPrefix),
				withPrefix: true,
				reason:     "stale channel removal markers would block re-watch",
			},
			{
				kind:       kindQueryCoordCache,
				key:        path.Join(c.basePath, collectionTargetPrefix),
				withPrefix: true,
				reason:     "cached query targets embed old-WAL positions; querycoord rebuilds them",
			},
		}
		for _, d := range candidates {
			n, err := c.countKeys(ctx, d.key)
			if err != nil {
				return nil, err
			}
			if n == 0 {
				continue
			}
			d.reason = fmt.Sprintf("%d key(s); %s", n, d.reason)
			deletes = append(deletes, d)
		}
	}

	return deletes, nil
}

// countKeys reports how many keys live under a prefix, so the plan only lists
// deletions that actually remove something.
func (c *ComponentReset) countKeys(ctx context.Context, prefix string) (int, error) {
	keys, _, err := c.client.LoadWithPrefix(ctx, prefix+"/", kv.WithKeysOnly())
	if err != nil {
		return 0, errors.Wrapf(err, "failed to scan %s", prefix)
	}
	return len(keys), nil
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
