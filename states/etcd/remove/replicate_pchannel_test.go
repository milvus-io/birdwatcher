package remove

import (
	"context"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"google.golang.org/protobuf/proto"

	_ "github.com/milvus-io/birdwatcher/asap"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/kv"
	commonpb "github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

var (
	testEtcdClient *clientv3.Client
	testKV         kv.MetaKV
)

func TestMain(m *testing.M) {
	cfg := embed.NewConfig()
	dir, _ := os.MkdirTemp("", "bw-remove-test-*")
	cfg.Dir = dir
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()
	defer os.RemoveAll(dir)

	select {
	case <-e.Server.ReadyNotify():
		testEtcdClient = v3client.New(e.Server)
		testKV = kv.NewEtcdKV(testEtcdClient)
	case <-time.After(60 * time.Second):
		e.Server.Stop()
		log.Fatal("etcd server took too long to start")
	}

	os.Exit(m.Run())
}

func seedReplicatePChannel(t *testing.T, basePath string, meta *streamingpb.ReplicatePChannelMeta, keyName string) {
	t.Helper()
	data, err := proto.Marshal(meta)
	require.NoError(t, err)
	fullKey := path.Join(basePath, "meta", "streamingcoord-meta/replicating-pchannel", keyName)
	err = testKV.Save(context.Background(), fullKey, string(data))
	require.NoError(t, err)
}

func TestRemoveReplicatePChannelDryRun(t *testing.T) {
	ctx := context.Background()
	basePath := "test-dryrun"
	defer testKV.RemoveWithPrefix(ctx, basePath)

	seedReplicatePChannel(t, basePath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_0",
		TargetChannelName: "rootcoord-dml_0",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-a"},
	}, "cluster-a-rootcoord-dml_0")

	seedReplicatePChannel(t, basePath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_1",
		TargetChannelName: "rootcoord-dml_1",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-b"},
	}, "cluster-b-rootcoord-dml_1")

	comp := &ComponentRemove{
		client:   testKV,
		basePath: path.Join(basePath, "meta"),
	}

	// Dry-run: should not delete anything
	err := comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		TargetCluster: "cluster-a",
	})
	assert.NoError(t, err)

	// Verify both keys still exist
	keys, _, err := testKV.LoadWithPrefix(ctx, path.Join(basePath, "meta", "streamingcoord-meta/replicating-pchannel"))
	assert.NoError(t, err)
	assert.Len(t, keys, 2)
}

func TestRemoveReplicatePChannelByTargetCluster(t *testing.T) {
	ctx := context.Background()
	basePath := "test-by-cluster"
	defer testKV.RemoveWithPrefix(ctx, basePath)

	seedReplicatePChannel(t, basePath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_0",
		TargetChannelName: "rootcoord-dml_0",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-a"},
	}, "cluster-a-rootcoord-dml_0")

	seedReplicatePChannel(t, basePath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_1",
		TargetChannelName: "rootcoord-dml_1",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-a"},
	}, "cluster-a-rootcoord-dml_1")

	seedReplicatePChannel(t, basePath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_2",
		TargetChannelName: "rootcoord-dml_2",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-b"},
	}, "cluster-b-rootcoord-dml_2")

	comp := &ComponentRemove{
		client:   testKV,
		basePath: path.Join(basePath, "meta"),
	}

	// Run with targetCluster filter
	err := comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetCluster:  "cluster-a",
	})
	assert.NoError(t, err)

	// Only cluster-b key should remain
	keys, _, err := testKV.LoadWithPrefix(ctx, path.Join(basePath, "meta", "streamingcoord-meta/replicating-pchannel"))
	assert.NoError(t, err)
	assert.Len(t, keys, 1)
	assert.Contains(t, keys[0], "cluster-b")
}

func TestRemoveReplicatePChannelBySourceChannel(t *testing.T) {
	ctx := context.Background()
	basePath := "test-by-source"
	defer testKV.RemoveWithPrefix(ctx, basePath)

	seedReplicatePChannel(t, basePath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_5",
		TargetChannelName: "rootcoord-dml_5",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-x"},
	}, "cluster-x-rootcoord-dml_5")

	seedReplicatePChannel(t, basePath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_6",
		TargetChannelName: "rootcoord-dml_6",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-x"},
	}, "cluster-x-rootcoord-dml_6")

	comp := &ComponentRemove{
		client:   testKV,
		basePath: path.Join(basePath, "meta"),
	}

	// Remove only dml_5
	err := comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		SourceChannel:  "dml_5",
	})
	assert.NoError(t, err)

	// Only dml_6 should remain
	keys, _, err := testKV.LoadWithPrefix(ctx, path.Join(basePath, "meta", "streamingcoord-meta/replicating-pchannel"))
	assert.NoError(t, err)
	assert.Len(t, keys, 1)
	assert.Contains(t, keys[0], "dml_6")
}

func TestRemoveReplicatePChannelNoMatch(t *testing.T) {
	ctx := context.Background()
	basePath := "test-no-match"
	defer testKV.RemoveWithPrefix(ctx, basePath)

	seedReplicatePChannel(t, basePath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_0",
		TargetChannelName: "rootcoord-dml_0",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-a"},
	}, "cluster-a-rootcoord-dml_0")

	comp := &ComponentRemove{
		client:   testKV,
		basePath: path.Join(basePath, "meta"),
	}

	// Filter that matches nothing
	err := comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetCluster:  "nonexistent",
	})
	assert.NoError(t, err)

	// Key should still exist
	keys, _, err := testKV.LoadWithPrefix(ctx, path.Join(basePath, "meta", "streamingcoord-meta/replicating-pchannel"))
	assert.NoError(t, err)
	assert.Len(t, keys, 1)
}

func TestRemoveReplicatePChannelEmpty(t *testing.T) {
	ctx := context.Background()
	basePath := "test-empty"
	defer testKV.RemoveWithPrefix(ctx, basePath)

	comp := &ComponentRemove{
		client:   testKV,
		basePath: path.Join(basePath, "meta"),
	}

	// No data at all
	err := comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
	})
	assert.NoError(t, err)
}

func TestRemoveReplicatePChannelAll(t *testing.T) {
	ctx := context.Background()
	basePath := "test-remove-all"
	defer testKV.RemoveWithPrefix(ctx, basePath)

	seedReplicatePChannel(t, basePath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_0",
		TargetChannelName: "rootcoord-dml_0",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-a"},
	}, "cluster-a-rootcoord-dml_0")

	seedReplicatePChannel(t, basePath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_1",
		TargetChannelName: "rootcoord-dml_1",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-b"},
	}, "cluster-b-rootcoord-dml_1")

	comp := &ComponentRemove{
		client:   testKV,
		basePath: path.Join(basePath, "meta"),
	}

	// No filter + run => remove all
	err := comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
	})
	assert.NoError(t, err)

	// All keys should be removed
	keys, _, err := testKV.LoadWithPrefix(ctx, path.Join(basePath, "meta", "streamingcoord-meta/replicating-pchannel"))
	assert.NoError(t, err)
	assert.Len(t, keys, 0)
}
