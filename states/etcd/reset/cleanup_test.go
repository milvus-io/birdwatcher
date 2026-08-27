package reset

import (
	"context"
	"path"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// a second shard, so scoping can be observed rather than assumed
const (
	otherPChannel = "by-dev-rootcoord-dml_1"
	otherVChannel = "by-dev-rootcoord-dml_1_440000000000000009v0"
	otherCollID   = int64(440000000000000009)
)

func collectionTargetKey(base string, collID int64) string {
	return path.Join(base, collectionTargetPrefix, strconv.FormatInt(collID, 10))
}

func channelRemovalKey(base, channel string) string {
	return path.Join(base, common.DCPrefix, common.ChannelRemovalPrefix, channel)
}

// seedCleanup extends the base fixture with a second pchannel and with the two
// kinds of adjacent state that used to be skipped entirely on a scoped run.
func seedCleanup(t *testing.T, name string) *fixture {
	t.Helper()
	ctx := context.Background()
	f := seed(t, name, commonpb.SegmentState_Flushed)

	save := func(key string, m proto.Message) {
		data, err := proto.Marshal(m)
		require.NoError(t, err)
		require.NoError(t, testKV.Save(ctx, key, string(data)))
	}

	save(path.Join(f.base, common.DBCollectionMetaPrefix, "1", strconv.FormatInt(otherCollID, 10)),
		&etcdpb.CollectionInfo{
			ID:                   otherCollID,
			Schema:               &schemapb.CollectionSchema{Name: "other"},
			VirtualChannelNames:  []string{otherVChannel},
			PhysicalChannelNames: []string{otherPChannel},
		})
	save(path.Join(f.base, "streamingcoord-meta/pchannel", otherPChannel),
		&streamingpb.PChannelMeta{
			Channel: &streamingpb.PChannelInfo{Name: otherPChannel, Term: 1},
		})
	save(common.ConsumeCheckpointKey(f.base, otherPChannel),
		&streamingpb.WALCheckpoint{
			MessageId:     &commonpb.MessageID{WALName: commonpb.WALName_Pulsar, Id: "CAEQAg=="},
			TimeTick:      testTimeTick,
			RecoveryMagic: 1,
		})

	// channel-removal markers are keyed by channel name...
	require.NoError(t, testKV.Save(ctx, channelRemovalKey(f.base, testVChannel), "removed"))
	require.NoError(t, testKV.Save(ctx, channelRemovalKey(f.base, otherVChannel), "removed"))
	// ...and query targets by collection id
	require.NoError(t, testKV.Save(ctx, collectionTargetKey(f.base, testCollID), "target"))
	require.NoError(t, testKV.Save(ctx, collectionTargetKey(f.base, otherCollID), "target"))

	return f
}

func exists(t *testing.T, key string) bool {
	t.Helper()
	_, err := testKV.Load(context.Background(), key)
	if err == nil {
		return true
	}
	require.ErrorIs(t, err, kv.ErrKeyNotFound)
	return false
}

// TestCleanupDropsTargetsOnWholeInstanceRun is the baseline: with no --pchannel,
// every marker and cached target goes.
func TestCleanupDropsTargetsOnWholeInstanceRun(t *testing.T) {
	f := seedCleanup(t, "cleanup-all")

	require.NoError(t, f.comp.ResetCheckpointCommand(context.Background(), &ResetCheckpointParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetWAL:      "woodpecker",
	}))

	assert.False(t, exists(t, collectionTargetKey(f.base, testCollID)))
	assert.False(t, exists(t, collectionTargetKey(f.base, otherCollID)))

	// channel-removal is NOT ours to delete — see the note in cleanup.go
	assert.True(t, exists(t, channelRemovalKey(f.base, testVChannel)))
	assert.True(t, exists(t, channelRemovalKey(f.base, otherVChannel)))
}

// TestCleanupNeverTouchesChannelRemoval pins a deletion that was removed after
// review. datacoord's channel-removal prefix reads like a set of stale tombstones
// but is really the NonRemoveFlagTomestone that MarkChannelAdded writes once at
// collection creation; MarkChannelDeleted has no callers today. Nothing rebuilds
// it, and datacoord reads it (ChannelExists) to hold back GC of dropped segments
// whose DML position is ahead of the channel checkpoint — so deleting it would
// permanently and silently weaken that guard.
func TestCleanupNeverTouchesChannelRemoval(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pchannel string
	}{
		{"whole-instance", ""},
		{"scoped", testPChannel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := seedCleanup(t, "cleanup-keep-removal-"+tc.name)

			require.NoError(t, f.comp.ResetCheckpointCommand(context.Background(), &ResetCheckpointParam{
				ExecutionParam: framework.ExecutionParam{Run: true},
				TargetWAL:      "woodpecker",
				PChannel:       tc.pchannel,
			}))

			assert.True(t, exists(t, channelRemovalKey(f.base, testVChannel)),
				"the live channel's added-marker must survive")
			assert.True(t, exists(t, channelRemovalKey(f.base, otherVChannel)))
		})
	}
}

// TestCleanupScopesTargetsToPChannel pins the fix for the stale-target bug: a
// --pchannel run used to skip both prefixes wholesale, leaving the reset
// collection's cached target full of old-WAL seek positions. querycoord recovers
// that cache verbatim and hands the positions to querynodes, so it has to go —
// while collections outside the scope must be left alone.
func TestCleanupScopesTargetsToPChannel(t *testing.T) {
	f := seedCleanup(t, "cleanup-scoped")

	require.NoError(t, f.comp.ResetCheckpointCommand(context.Background(), &ResetCheckpointParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetWAL:      "woodpecker",
		PChannel:       testPChannel,
	}))

	assert.False(t, exists(t, collectionTargetKey(f.base, testCollID)),
		"the reset collection's cached target embeds old-WAL positions and must be dropped")
	assert.True(t, exists(t, collectionTargetKey(f.base, otherCollID)),
		"a collection outside --pchannel must not be touched")
}

// TestCleanupDryRunDeletesNothing guards the dry-run contract for the delete
// half of the plan, which is easy to bypass when adding new deletions.
func TestCleanupDryRunDeletesNothing(t *testing.T) {
	f := seedCleanup(t, "cleanup-dry")

	require.NoError(t, f.comp.ResetCheckpointCommand(context.Background(), &ResetCheckpointParam{
		TargetWAL: "woodpecker",
	}))

	assert.True(t, exists(t, collectionTargetKey(f.base, testCollID)))
}

// TestCleanupSweepsOrphanTargetOnWholeInstanceRun pins why a whole-instance run
// drops the whole prefix instead of enumerating collections: a collection whose
// meta is already gone leaves its cached target behind, and ListCollections
// cannot see it. A scoped run cannot attribute such an orphan to a pchannel, so
// it leaves it alone.
func TestCleanupSweepsOrphanTargetOnWholeInstanceRun(t *testing.T) {
	const orphanCollID = int64(440000000000000099)

	t.Run("whole-instance sweeps it", func(t *testing.T) {
		f := seedCleanup(t, "cleanup-orphan-all")
		require.NoError(t, testKV.Save(context.Background(),
			collectionTargetKey(f.base, orphanCollID), "target"))

		require.NoError(t, f.comp.ResetCheckpointCommand(context.Background(), &ResetCheckpointParam{
			ExecutionParam: framework.ExecutionParam{Run: true},
			TargetWAL:      "woodpecker",
		}))

		assert.False(t, exists(t, collectionTargetKey(f.base, orphanCollID)),
			"a target with no surviving collection meta must still be swept")
	})

	t.Run("scoped run leaves it", func(t *testing.T) {
		f := seedCleanup(t, "cleanup-orphan-scoped")
		require.NoError(t, testKV.Save(context.Background(),
			collectionTargetKey(f.base, orphanCollID), "target"))

		require.NoError(t, f.comp.ResetCheckpointCommand(context.Background(), &ResetCheckpointParam{
			ExecutionParam: framework.ExecutionParam{Run: true},
			TargetWAL:      "woodpecker",
			PChannel:       testPChannel,
		}))

		assert.True(t, exists(t, collectionTargetKey(f.base, orphanCollID)),
			"an orphan cannot be attributed to a pchannel, so a scoped run must not guess")
	})
}
