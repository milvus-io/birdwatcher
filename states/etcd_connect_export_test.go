package states

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
)

func TestGetKVConnectedStateReturnsMetastoreState(t *testing.T) {
	config := &configs.Config{}
	core := framework.NewCmdState("[core]", config)
	client := metakv.NewFileAuditKV(nil, nil)

	state := GetKVConnectedState(core, client, "scout:2379", config, nil, nil)
	require.NotNil(t, state)
	require.Contains(t, state.Label(), "MetaStore(scout:2379)")
}

func TestDefaultMetaPathIsMeta(t *testing.T) {
	require.Equal(t, "meta", DefaultMetaPath)
}

func TestPingMetaStoreIsExported(t *testing.T) {
	require.NotNil(t, PingMetaStore)
}

func TestExportedStateTags(t *testing.T) {
	require.Equal(t, "etcd", EtcdTag)
	require.Equal(t, "tikv", TiKVTag)
	require.Equal(t, "pulsar", PulsarTag)
	require.Equal(t, "oss", OSSTag)
}
