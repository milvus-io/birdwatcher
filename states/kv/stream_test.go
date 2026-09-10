package kv

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// TestEtcdKVGetStream verifies the RangeStream-based root path enumeration over
// a real gRPC connection to an etcd 3.7 server. Note the in-process v3client
// adapter used by other tests returns Unimplemented for RangeStream, so those
// exercise the legacy unary fallback instead.
func TestEtcdKVGetStream(t *testing.T) {
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "warn"
	cfg.LogOutputs = []string{"default"}
	u, err := url.Parse("http://localhost:0")
	require.NoError(t, err)
	cfg.ListenClientUrls = []url.URL{*u}
	cfg.ListenPeerUrls = []url.URL{*u}
	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	defer e.Close()

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("embed etcd not ready")
	}

	addr := e.Clients[0].Addr().String()
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{addr}, DialTimeout: 5 * time.Second, Logger: zap.NewNop()})
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	kvCli := NewEtcdKV(c)
	for k, v := range map[string]string{"testr1": "v1", "testr1/a": "va", "testr2": "v2", "testr3": "v3"} {
		require.NoError(t, kvCli.Save(ctx, k, v))
	}

	stream, err := kvCli.GetStream(ctx, "", clientv3.WithKeysOnly(), clientv3.WithLimit(1), clientv3.WithFromKey())
	require.NoError(t, err)
	keys := make([]string, 0)
	for chunk := range stream {
		require.NoError(t, chunk.Err)
		for _, k := range chunk.Keys {
			keys = append(keys, string(k))
		}
	}
	require.Contains(t, keys, "testr1")

	roots, err := kvCli.GetAllRootPath(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"testr1", "testr2", "testr3"}, roots)
}
