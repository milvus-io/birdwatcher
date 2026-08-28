package reset

import (
	"context"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"google.golang.org/protobuf/proto"

	_ "github.com/milvus-io/birdwatcher/asap"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

var testKV kv.MetaKV

func TestMain(m *testing.M) {
	cfg := embed.NewConfig()
	dir, _ := os.MkdirTemp("", "bw-reset-test-*")
	cfg.Dir = dir
	cfg.LogLevel = "error"
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		os.RemoveAll(dir)
		log.Fatal(err)
	}
	select {
	case <-e.Server.ReadyNotify():
		testKV = kv.NewEtcdKV(v3client.New(e.Server))
	case <-time.After(60 * time.Second):
		e.Server.Stop()
		os.RemoveAll(dir)
		log.Fatal("etcd server took too long to start")
	}
	code := m.Run()
	e.Close()
	os.RemoveAll(dir)
	os.Exit(code)
}

const (
	testPChannel = "by-dev-rootcoord-dml_0"
	testVChannel = "by-dev-rootcoord-dml_0_440000000000000001v0"
	testCollID   = int64(440000000000000001)
	testPartID   = int64(440000000000000002)
	testSegID    = int64(440000000000000003)
	testTimeTick = uint64(450000000000000000)
)

// pulsarPos is a stand-in for a position left behind by the old MQ. The exact
// bytes do not matter; what matters is that reset replaces them and tags the
// result with the new WAL name.
func pulsarPos(channel string) *msgpb.MsgPosition {
	return &msgpb.MsgPosition{
		ChannelName: channel,
		MsgID:       []byte{0x08, 0x01, 0x10, 0x02, 0x18, 0x00},
		Timestamp:   testTimeTick,
		WALName:     commonpb.WALName_Pulsar,
	}
}

type fixture struct {
	base string
	comp *ComponentReset
}

// seed writes one collection, one segment, one channel checkpoint, one consume
// checkpoint and one segment allocation — i.e. every kind reset has to handle.
func seed(t *testing.T, name string, segState commonpb.SegmentState) *fixture {
	t.Helper()
	ctx := context.Background()
	base := path.Join(name, "meta")
	t.Cleanup(func() { testKV.RemoveWithPrefix(ctx, name) })

	save := func(key string, m proto.Message) {
		data, err := proto.Marshal(m)
		require.NoError(t, err)
		require.NoError(t, testKV.Save(ctx, key, string(data)))
	}

	save(path.Join(base, common.DBCollectionMetaPrefix, "1", "440000000000000001"),
		&etcdpb.CollectionInfo{
			ID: testCollID,
			// a real collection always carries a schema; ListCollections
			// dereferences it unconditionally (common/collection.go:132)
			Schema:               &schemapb.CollectionSchema{Name: "probe"},
			VirtualChannelNames:  []string{testVChannel},
			PhysicalChannelNames: []string{testPChannel},
			StartPositions: []*commonpb.KeyDataPair{
				{Key: testPChannel, Data: []byte{0x08, 0x01}},
			},
		})

	save(path.Join(base, common.DCPrefix, common.SegmentMetaPrefix,
		"440000000000000001", "440000000000000002", "440000000000000003"),
		&datapb.SegmentInfo{
			ID:            testSegID,
			CollectionID:  testCollID,
			PartitionID:   testPartID,
			InsertChannel: testVChannel,
			State:         segState,
			StartPosition: pulsarPos(testVChannel),
			DmlPosition:   pulsarPos(testVChannel),
		})

	save(path.Join(base, common.DCPrefix, common.ChannelCheckpointPrefix, testVChannel),
		pulsarPos(testVChannel))

	save(path.Join(base, "streamingcoord-meta/pchannel", testPChannel),
		&streamingpb.PChannelMeta{
			Channel: &streamingpb.PChannelInfo{Name: testPChannel, Term: 1},
		})

	save(common.ConsumeCheckpointKey(base, testPChannel),
		&streamingpb.WALCheckpoint{
			MessageId:     &commonpb.MessageID{WALName: commonpb.WALName_Pulsar, Id: "CAEQAg=="},
			TimeTick:      testTimeTick,
			RecoveryMagic: 1,
		})

	save(path.Join(common.SegmentAssignPrefix(base, testPChannel), "440000000000000003"),
		&streamingpb.SegmentAssignmentMeta{SegmentId: testSegID, Vchannel: testVChannel})

	return &fixture{base: base, comp: &ComponentReset{client: testKV, basePath: base}}
}

func (f *fixture) load(t *testing.T, key string, m proto.Message) {
	t.Helper()
	val, err := testKV.Load(context.Background(), key)
	require.NoError(t, err)
	require.NoError(t, proto.Unmarshal([]byte(val), m))
}

// assertMsgID compares message ids by value, treating nil and empty as the same
// thing: protobuf drops a zero-length bytes field entirely, so woodpecker's
// earliest id reads back as nil. Milvus makes the same equivalence — it tests
// len(MsgID) != 0, never MsgID != nil.
func assertMsgID(t *testing.T, want, got []byte, msgAndArgs ...any) {
	t.Helper()
	if len(want) == 0 {
		assert.Empty(t, got, msgAndArgs...)
		return
	}
	assert.Equal(t, want, got, msgAndArgs...)
}

func TestResetRewritesEveryPositionKind(t *testing.T) {
	f := seed(t, "reset-all", commonpb.SegmentState_Flushed)

	err := f.comp.ResetCheckpointCommand(context.Background(), &ResetCheckpointParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetWAL:      "woodpecker",
	})
	require.NoError(t, err)

	want, err := buildWALPosition("woodpecker")
	require.NoError(t, err)

	t.Run("consume-checkpoint", func(t *testing.T) {
		cp := &streamingpb.WALCheckpoint{}
		f.load(t, common.ConsumeCheckpointKey(f.base, testPChannel), cp)
		assert.Equal(t, commonpb.WALName_WoodPecker, cp.GetMessageId().GetWALName())
		assert.Equal(t, want.msgID.Id, cp.GetMessageId().GetId())
		assert.Equal(t, testTimeTick, cp.GetTimeTick(), "TimeTick is a global TSO and must survive")
		assert.EqualValues(t, 1, cp.GetRecoveryMagic(), "recovery magic must survive")
		assert.Nil(t, cp.GetAlterWalState(), "any interrupted online switch must be cleared")
	})

	t.Run("channel-cp", func(t *testing.T) {
		pos := &msgpb.MsgPosition{}
		f.load(t, path.Join(f.base, common.DCPrefix, common.ChannelCheckpointPrefix, testVChannel), pos)
		assertMsgID(t, want.raw, pos.GetMsgID())
		assert.Equal(t, commonpb.WALName_WoodPecker, pos.GetWALName(),
			"WALName must be rewritten too, otherwise milvus decodes the new id with the old codec")
		assert.Equal(t, testTimeTick, pos.GetTimestamp())
		assert.Equal(t, testVChannel, pos.GetChannelName())
	})

	t.Run("segment positions", func(t *testing.T) {
		seg := &datapb.SegmentInfo{}
		f.load(t, path.Join(f.base, common.DCPrefix, common.SegmentMetaPrefix,
			"440000000000000001", "440000000000000002", "440000000000000003"), seg)
		for name, pos := range map[string]*msgpb.MsgPosition{
			"start": seg.GetStartPosition(), "dml": seg.GetDmlPosition(),
		} {
			assertMsgID(t, want.raw, pos.GetMsgID(), name)
			assert.Equal(t, commonpb.WALName_WoodPecker, pos.GetWALName(), name)
			assert.Equal(t, testTimeTick, pos.GetTimestamp(), name)
		}
	})

	t.Run("collection start positions", func(t *testing.T) {
		coll := &etcdpb.CollectionInfo{}
		f.load(t, path.Join(f.base, common.DBCollectionMetaPrefix, "1", "440000000000000001"), coll)
		require.Len(t, coll.GetStartPositions(), 1)
		assert.Equal(t, testPChannel, coll.GetStartPositions()[0].GetKey())
		assertMsgID(t, want.raw, coll.GetStartPositions()[0].GetData())
	})

	t.Run("segment-assign dropped", func(t *testing.T) {
		keys, _, err := testKV.LoadWithPrefix(context.Background(),
			common.SegmentAssignPrefix(f.base, testPChannel)+"/")
		require.NoError(t, err)
		assert.Empty(t, keys, "stale growing-segment allocations must not survive a rewind")
	})
}

func TestResetDryRunWritesNothing(t *testing.T) {
	f := seed(t, "reset-dry", commonpb.SegmentState_Flushed)
	key := path.Join(f.base, common.DCPrefix, common.ChannelCheckpointPrefix, testVChannel)

	before := &msgpb.MsgPosition{}
	f.load(t, key, before)

	err := f.comp.ResetCheckpointCommand(context.Background(), &ResetCheckpointParam{
		TargetWAL: "woodpecker",
	})
	require.NoError(t, err)

	after := &msgpb.MsgPosition{}
	f.load(t, key, after)
	assert.Equal(t, commonpb.WALName_Pulsar, after.GetWALName(), "dry run must not touch anything")
	assert.Equal(t, before.GetMsgID(), after.GetMsgID())

	keys, _, err := testKV.LoadWithPrefix(context.Background(),
		common.SegmentAssignPrefix(f.base, testPChannel)+"/")
	require.NoError(t, err)
	assert.Len(t, keys, 1, "dry run must not delete either")
}

func TestResetRefusesGrowingSegments(t *testing.T) {
	f := seed(t, "reset-growing", commonpb.SegmentState_Growing)

	err := f.comp.ResetCheckpointCommand(context.Background(), &ResetCheckpointParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetWAL:      "woodpecker",
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "growing segment")

	// and nothing was written before the refusal
	cp := &streamingpb.WALCheckpoint{}
	f.load(t, common.ConsumeCheckpointKey(f.base, testPChannel), cp)
	assert.Equal(t, commonpb.WALName_Pulsar, cp.GetMessageId().GetWALName())
}

func TestResetAllowGrowingOverride(t *testing.T) {
	f := seed(t, "reset-growing-ok", commonpb.SegmentState_Growing)

	err := f.comp.ResetCheckpointCommand(context.Background(), &ResetCheckpointParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetWAL:      "woodpecker",
		AllowGrowing:   true,
	})
	require.NoError(t, err)

	cp := &streamingpb.WALCheckpoint{}
	f.load(t, common.ConsumeCheckpointKey(f.base, testPChannel), cp)
	assert.Equal(t, commonpb.WALName_WoodPecker, cp.GetMessageId().GetWALName())
}

// Running twice must be a no-op the second time. Before this was enforced the
// plan still listed every key, which reads as "the first run did not take".
func TestResetIsIdempotent(t *testing.T) {
	f := seed(t, "reset-idem", commonpb.SegmentState_Flushed)
	param := func() *ResetCheckpointParam {
		return &ResetCheckpointParam{
			ExecutionParam: framework.ExecutionParam{Run: true},
			TargetWAL:      "woodpecker",
		}
	}
	require.NoError(t, f.comp.ResetCheckpointCommand(context.Background(), param()))

	writes, err := f.comp.plan(context.Background(), param(), mustPos(t))
	require.NoError(t, err)
	assert.Empty(t, writes, "second run must find nothing left to rewrite")

	deletes, err := f.comp.planCleanup(context.Background(), param())
	require.NoError(t, err)
	assert.Empty(t, deletes, "and nothing left to delete")

	// still applies cleanly
	require.NoError(t, f.comp.ResetCheckpointCommand(context.Background(), param()))
}

func mustPos(t *testing.T) *walPosition {
	t.Helper()
	pos, err := buildWALPosition("woodpecker")
	require.NoError(t, err)
	return pos
}

// A run that dies after the channel checkpoints but before the consume
// checkpoint leaves the two sides disagreeing. Milvus panics on startup in that
// state (verified end to end), so the command has to name it rather than just
// listing keys.
func TestResetDetectsSplitStateAndConverges(t *testing.T) {
	ctx := context.Background()
	f := seed(t, "reset-split", commonpb.SegmentState_Flushed)
	param := func() *ResetCheckpointParam {
		return &ResetCheckpointParam{
			ExecutionParam: framework.ExecutionParam{Run: true},
			TargetWAL:      "woodpecker",
		}
	}

	// no split before anything ran
	_, split := f.comp.detectSplitState(ctx, param())
	assert.False(t, split, "a consistent instance is not a split state")

	require.NoError(t, f.comp.ResetCheckpointCommand(ctx, param()))

	// rewind only the consume checkpoint, reproducing a partial run
	cp := &streamingpb.WALCheckpoint{}
	f.load(t, common.ConsumeCheckpointKey(f.base, testPChannel), cp)
	cp.MessageId = &commonpb.MessageID{WALName: commonpb.WALName_Pulsar, Id: "CAEQAg=="}
	require.NoError(t, common.SaveConsumeCheckpoint(ctx, testKV, f.base, testPChannel, cp))

	detail, split := f.comp.detectSplitState(ctx, param())
	require.True(t, split, "channel-cp and consume-checkpoint now disagree")
	assert.Contains(t, detail, "WoodPecker")
	assert.Contains(t, detail, "Pulsar")

	// re-running must fix exactly the missing side and then converge
	writes, err := f.comp.plan(ctx, param(), mustPos(t))
	require.NoError(t, err)
	require.Len(t, writes, 1, "only the consume checkpoint is left to write")
	assert.Equal(t, kindConsumeCP, writes[0].kind)

	require.NoError(t, f.comp.ResetCheckpointCommand(ctx, param()))
	_, split = f.comp.detectSplitState(ctx, param())
	assert.False(t, split, "re-run converges the split state")
}

// The two sides are keyed differently — channel-cp is per vchannel, the consume
// checkpoint is per pchannel — so their key counts differ on any real instance.
// Comparing counts instead of WAL names reports every healthy instance as split.
func TestDetectSplitStateIgnoresKeyCounts(t *testing.T) {
	ctx := context.Background()
	f := seed(t, "reset-counts", commonpb.SegmentState_Flushed)
	param := &ResetCheckpointParam{TargetWAL: "woodpecker"}

	// add a second vchannel checkpoint on the same pchannel: 2 channel-cp keys
	// against 1 consume-checkpoint key, both still Pulsar.
	extra := pulsarPos(testPChannel + "_440000000000000001v1")
	data, err := proto.Marshal(extra)
	require.NoError(t, err)
	require.NoError(t, testKV.Save(ctx, path.Join(f.base, common.DCPrefix,
		common.ChannelCheckpointPrefix, extra.ChannelName), string(data)))

	_, split := f.comp.detectSplitState(ctx, param)
	assert.False(t, split, "differing key counts with the same WAL name is not a split state")
}

// TestResetPreservesReplicateCheckpoint pins the one field in WALCheckpoint that
// this command must not follow the local WAL with. ReplicateCheckpoint is a
// subscription position against a remote cluster, owned by Milvus; switching the
// local WAL says nothing about it, so reset leaves it exactly as found.
func TestResetPreservesReplicateCheckpoint(t *testing.T) {
	ctx := context.Background()
	f := seed(t, "reset-replicate", commonpb.SegmentState_Flushed)

	remote := &commonpb.ReplicateCheckpoint{
		ClusterId: "source-cluster",
		Pchannel:  "src-rootcoord-dml_0",
		MessageId: &commonpb.MessageID{WALName: commonpb.WALName_Pulsar, Id: "CAEQBg=="},
		TimeTick:  testTimeTick - 1,
	}
	key := common.ConsumeCheckpointKey(f.base, testPChannel)
	data, err := proto.Marshal(&streamingpb.WALCheckpoint{
		MessageId:           &commonpb.MessageID{WALName: commonpb.WALName_Pulsar, Id: "CAEQAg=="},
		TimeTick:            testTimeTick,
		RecoveryMagic:       1,
		ReplicateCheckpoint: remote,
	})
	require.NoError(t, err)
	require.NoError(t, testKV.Save(ctx, key, string(data)))

	require.NoError(t, f.comp.ResetCheckpointCommand(ctx, &ResetCheckpointParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetWAL:      "woodpecker",
	}))

	cp := &streamingpb.WALCheckpoint{}
	f.load(t, key, cp)

	// the local position did move
	assert.Equal(t, commonpb.WALName_WoodPecker, cp.GetMessageId().GetWALName())
	// ...and the remote one did not
	assert.True(t, proto.Equal(remote, cp.GetReplicateCheckpoint()),
		"ReplicateCheckpoint is Milvus-owned remote state and must survive untouched, got %v",
		cp.GetReplicateCheckpoint())
}

// TestResetRunsWithReplicationConfigured guards against reintroducing a hard
// refusal on replicating instances. Replication topology is not this command's
// concern: the two clusters are independent, and Milvus puts no such gate on its
// own WAL switch.
func TestResetRunsWithReplicationConfigured(t *testing.T) {
	ctx := context.Background()
	f := seed(t, "reset-replicate-cfg", commonpb.SegmentState_Flushed)

	save := func(key string, m proto.Message) {
		data, err := proto.Marshal(m)
		require.NoError(t, err)
		require.NoError(t, testKV.Save(ctx, key, string(data)))
	}
	save(path.Join(f.base, "streamingcoord-meta/replicate-configuration"),
		&streamingpb.ReplicateConfigurationMeta{
			ReplicateConfiguration: &commonpb.ReplicateConfiguration{
				Clusters: []*commonpb.MilvusCluster{
					{ClusterId: "source-cluster"}, {ClusterId: "by-dev"},
				},
			},
		})
	save(path.Join(f.base, "streamingcoord-meta/replicating-pchannel", testPChannel),
		&streamingpb.ReplicatePChannelMeta{SourceChannelName: "src-rootcoord-dml_0"})

	require.NoError(t, f.comp.preflight(ctx, &ResetCheckpointParam{TargetWAL: "woodpecker"}),
		"a replicating instance must still be resettable")
}
