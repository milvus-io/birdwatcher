package kv

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/tikv/client-go/v2/testutils"
	tilib "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"

	_ "github.com/milvus-io/birdwatcher/asap"
)

var (
	txnClient  *txnkv.Client
	etcdClient *clientv3.Client
	kvClients  []MetaKV
)

// creates a local TiKV Store for testing purpose.
func setupLocalTiKV() {
	setupLocalTxn()
	setupLocalEtcd()
	kvClients = []MetaKV{NewTiKV(txnClient), NewEtcdKV(etcdClient)}
}

func setupLocalEtcd() {
	cfg := embed.NewConfig()
	cfg.Dir = "/tmp/default.etcd"
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Fatal(err)
	}
	select {
	case <-e.Server.ReadyNotify():
		log.Printf("Server is ready!")
		etcdClient = v3client.New(e.Server)
		break
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		log.Printf("Server took too long to start!")
	}
}

func setupLocalTxn() {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	if err != nil {
		panic(err)
	}
	testutils.BootstrapWithSingleStore(cluster)
	store, err := tilib.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	if err != nil {
		panic(err)
	}
	txnClient = &txnkv.Client{KVStore: store}
}

// Connects to a remote TiKV service for testing purpose. By default, it assumes the TiKV is from localhost.
func setupRemoteTiKV() {
	pdsn := "127.0.0.1:2379"
	var err error
	txnClient, err = txnkv.NewClient([]string{pdsn})
	if err != nil {
		panic(err)
	}
}

func setupTiKV(useRemote bool) {
	if useRemote {
		setupRemoteTiKV()
	} else {
		setupLocalTiKV()
	}
}

func TestMain(m *testing.M) {
	setupTiKV(false)
	code := m.Run()
	os.Exit(code)
}
