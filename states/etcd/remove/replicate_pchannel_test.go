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
		os.RemoveAll(dir)
		log.Fatal(err)
	}

	select {
	case <-e.Server.ReadyNotify():
		testEtcdClient = v3client.New(e.Server)
		testKV = kv.NewEtcdKV(testEtcdClient)
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

func seedReplicatePChannel(t *testing.T, metaPath string, meta *streamingpb.ReplicatePChannelMeta, keyName string) {
	t.Helper()
	data, err := proto.Marshal(meta)
	require.NoError(t, err)
	fullKey := path.Join(metaPath, "streamingcoord-meta/replicating-pchannel", keyName)
	err = testKV.Save(context.Background(), fullKey, string(data))
	require.NoError(t, err)
}

func seedReplicateConfig(t *testing.T, metaPath string, clusters []*commonpb.MilvusCluster, topos []*commonpb.CrossClusterTopology) {
	t.Helper()
	cfgMeta := &streamingpb.ReplicateConfigurationMeta{
		ReplicateConfiguration: &commonpb.ReplicateConfiguration{
			Clusters:             clusters,
			CrossClusterTopology: topos,
		},
	}
	data, err := proto.Marshal(cfgMeta)
	require.NoError(t, err)
	fullKey := path.Join(metaPath, "streamingcoord-meta/replicate-configuration")
	err = testKV.Save(context.Background(), fullKey, string(data))
	require.NoError(t, err)
}

func newComp(metaPath string) *ComponentRemove {
	return &ComponentRemove{
		client:   testKV,
		basePath: metaPath, // not used by this command
		metaPath: metaPath,
	}
}

func TestRemoveReplicatePChannelRequiresBothFilters(t *testing.T) {
	ctx := context.Background()
	comp := newComp("test-require-filter/meta")

	// No filter at all
	err := comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
	})
	assert.NoError(t, err)

	// Only targetCluster
	err = comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetCluster:  "cluster-a",
	})
	assert.NoError(t, err)

	// Only sourceChannel
	err = comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		SourceChannel:  "dml_0",
	})
	assert.NoError(t, err)
}

func TestRemoveReplicatePChannelDryRun(t *testing.T) {
	ctx := context.Background()
	metaPath := "test-dryrun/meta"
	defer testKV.RemoveWithPrefix(ctx, "test-dryrun")

	// cluster-b is in config, cluster-a is NOT (dirty)
	seedReplicateConfig(t, metaPath,
		[]*commonpb.MilvusCluster{{ClusterId: "cluster-b"}},
		nil,
	)
	seedReplicatePChannel(t, metaPath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_0",
		TargetChannelName: "rootcoord-dml_0",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-a"},
	}, "cluster-a-rootcoord-dml_0")

	comp := newComp(metaPath)

	// Dry-run: should not delete anything
	err := comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		TargetCluster: "cluster-a",
		SourceChannel: "dml_0",
	})
	assert.NoError(t, err)

	// Key should still exist
	keys, _, err := testKV.LoadWithPrefix(ctx, path.Join(metaPath, "streamingcoord-meta/replicating-pchannel"))
	assert.NoError(t, err)
	assert.Len(t, keys, 1)
}

func TestRemoveReplicatePChannelOnlyDirty(t *testing.T) {
	ctx := context.Background()
	metaPath := "test-only-dirty/meta"
	defer testKV.RemoveWithPrefix(ctx, "test-only-dirty")

	// cluster-b is valid (in config), cluster-a is dirty (not in config)
	seedReplicateConfig(t, metaPath,
		[]*commonpb.MilvusCluster{{ClusterId: "cluster-b"}},
		nil,
	)

	seedReplicatePChannel(t, metaPath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_0",
		TargetChannelName: "rootcoord-dml_0",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-a"},
	}, "cluster-a-rootcoord-dml_0")

	seedReplicatePChannel(t, metaPath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_1",
		TargetChannelName: "rootcoord-dml_1",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-b"},
	}, "cluster-b-rootcoord-dml_1")

	comp := newComp(metaPath)

	// Broad filter matches both => should refuse because cluster-b is valid
	err := comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetCluster:  "cluster",
		SourceChannel:  "rootcoord-dml",
	})
	assert.NoError(t, err)

	// Nothing should be deleted
	keys, _, err := testKV.LoadWithPrefix(ctx, path.Join(metaPath, "streamingcoord-meta/replicating-pchannel"))
	assert.NoError(t, err)
	assert.Len(t, keys, 2)

	// Now use precise filter for only dirty cluster-a
	err = comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetCluster:  "cluster-a",
		SourceChannel:  "rootcoord-dml",
	})
	assert.NoError(t, err)

	// Only cluster-b (valid) should remain
	keys, _, err = testKV.LoadWithPrefix(ctx, path.Join(metaPath, "streamingcoord-meta/replicating-pchannel"))
	assert.NoError(t, err)
	assert.Len(t, keys, 1)
	assert.Contains(t, keys[0], "cluster-b")
}

func TestRemoveReplicatePChannelRefuseValidTarget(t *testing.T) {
	ctx := context.Background()
	metaPath := "test-refuse-valid/meta"
	defer testKV.RemoveWithPrefix(ctx, "test-refuse-valid")

	// cluster-a is in config (valid)
	seedReplicateConfig(t, metaPath,
		[]*commonpb.MilvusCluster{{ClusterId: "cluster-a"}},
		nil,
	)

	seedReplicatePChannel(t, metaPath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_0",
		TargetChannelName: "rootcoord-dml_0",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-a"},
	}, "cluster-a-rootcoord-dml_0")

	comp := newComp(metaPath)

	// Try to delete cluster-a which is valid => should refuse
	err := comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetCluster:  "cluster-a",
		SourceChannel:  "dml_0",
	})
	assert.NoError(t, err)

	// Key should still exist
	keys, _, err := testKV.LoadWithPrefix(ctx, path.Join(metaPath, "streamingcoord-meta/replicating-pchannel"))
	assert.NoError(t, err)
	assert.Len(t, keys, 1)
}

func TestRemoveReplicatePChannelNoConfig(t *testing.T) {
	ctx := context.Background()
	metaPath := "test-no-config/meta"
	defer testKV.RemoveWithPrefix(ctx, "test-no-config")

	// No config seeded — all targets are dirty
	seedReplicatePChannel(t, metaPath, &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "rootcoord-dml_0",
		TargetChannelName: "rootcoord-dml_0",
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "cluster-x"},
	}, "cluster-x-rootcoord-dml_0")

	comp := newComp(metaPath)

	err := comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetCluster:  "cluster-x",
		SourceChannel:  "dml_0",
	})
	assert.NoError(t, err)

	// Should be removed since no config means all are dirty
	keys, _, err := testKV.LoadWithPrefix(ctx, path.Join(metaPath, "streamingcoord-meta/replicating-pchannel"))
	assert.NoError(t, err)
	assert.Len(t, keys, 0)
}

func TestRemoveReplicatePChannelEmpty(t *testing.T) {
	ctx := context.Background()
	metaPath := "test-empty/meta"
	defer testKV.RemoveWithPrefix(ctx, "test-empty")

	comp := newComp(metaPath)

	err := comp.RemoveReplicatePChannelCommand(ctx, &RemoveReplicatePChannelParam{
		ExecutionParam: framework.ExecutionParam{Run: true},
		TargetCluster:  "any",
		SourceChannel:  "any",
	})
	assert.NoError(t, err)
}
