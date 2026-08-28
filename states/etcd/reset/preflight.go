package reset

import (
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// preflight refuses to touch anything while the instance is in a state where a
// position reset would silently destroy data. Each check either passes or
// explains what the operator has to do first.
func (c *ComponentReset) preflight(ctx context.Context, p *ResetCheckpointParam) error {
	if err := c.checkNoGrowingSegments(ctx, p); err != nil {
		return err
	}
	return c.checkNoPendingBroadcast(ctx)
}

// checkNoGrowingSegments refuses to run while growing segments exist: their rows
// only live in the old MQ, so resetting positions would drop them for good.
func (c *ComponentReset) checkNoGrowingSegments(ctx context.Context, p *ResetCheckpointParam) error {
	segments, err := common.ListSegments(ctx, c.client, c.basePath, func(s *models.Segment) bool {
		return s.State == commonpb.SegmentState_Growing && c.inScope(p, s.InsertChannel)
	})
	if err != nil {
		return errors.Wrap(err, "failed to list segments")
	}
	if len(segments) == 0 {
		return nil
	}

	ids := make([]int64, 0, len(segments))
	for _, s := range segments {
		ids = append(ids, s.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	if !p.AllowGrowing {
		return errors.Newf("found %d growing segment(s) %v: their rows are still only in the old MQ. "+
			"Flush every collection before switching, or pass --allow-growing to accept losing them", len(ids), ids)
	}
	fmt.Printf("WARNING: continuing with %d growing segment(s) %v; their unflushed rows will be lost\n", len(ids), ids)
	return nil
}

// checkNoPendingBroadcast refuses to run while a DDL broadcast is half-applied.
// Rewinding positions would replay it against a WAL that no longer has it.
func (c *ComponentReset) checkNoPendingBroadcast(ctx context.Context) error {
	// An instance with no streaming metadata at all — a pre-2.6 one, say — simply
	// yields an empty list here, which passes the check below.
	tasks, err := common.ListWalBroadcast(ctx, c.client, c.basePath)
	if err != nil {
		return errors.Wrap(err, "failed to list wal broadcast tasks")
	}

	pending := 0
	for _, t := range tasks {
		if t.GetState() != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
			pending++
		}
	}
	if pending > 0 {
		return errors.Newf("found %d in-flight broadcast task(s): a DDL is half-applied and the "+
			"instance is inconsistent. Bring Milvus up once to let it settle, then stop it and retry", pending)
	}
	return nil
}
