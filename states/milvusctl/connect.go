package milvusctl

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type ConnectMilvusParam struct {
	framework.ParamBase `use:"connect milvus" desc:"connect to a Milvus instance"`
	Address             string `name:"address" default:"localhost:19530" desc:"Milvus server address (host:port)"`
	Username            string `name:"username" default:"" desc:"auth username"`
	Password            string `name:"password" default:"" desc:"auth password"`
	APIKey              string `name:"api-key" default:"" desc:"API key (overrides user/pass)"`
	DB                  string `name:"db" default:"" desc:"database name"`
	TLS                 bool   `name:"tls" default:"false" desc:"enable TLS"`
	Timeout             string `name:"timeout" default:"10s" desc:"dial timeout, e.g. 10s / 500ms; 0 disables"`
}

func ConnectMilusctl(ctx context.Context, p *ConnectMilvusParam, parent *framework.CmdState) (*MilvusctlState, error) {
	cfg := &milvusclient.ClientConfig{
		Address:       p.Address,
		Username:      p.Username,
		Password:      p.Password,
		APIKey:        p.APIKey,
		DBName:        p.DB,
		EnableTLSAuth: p.TLS,
	}
	cli, err := milvusclient.New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &MilvusctlState{
		CmdState:  parent.Spawn(fmt.Sprintf("Milvusctl[%s]", cfg.Address)),
		clientcfg: cfg,
		client:    cli,
	}, nil
}

type MilvusctlState struct {
	*framework.CmdState
	clientcfg *milvusclient.ClientConfig
	client    *milvusclient.Client
}

// Label overrides default cmd label behavior
// returning OSS[PROVIDER](BUCKET/CURR_DIR)
func (s *MilvusctlState) Label() string {
	return fmt.Sprintf("Milvusctl[%s]", s.clientcfg.Address)
}

func (s *MilvusctlState) Close() {
	if s.client != nil {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_ = s.client.Close(ctx)
	}
}

func (s *MilvusctlState) SetupCommands() {
	cmd := s.GetCmd()

	s.MergeFunctionCommands(cmd, s)
	s.UpdateState(cmd, s, s.SetupCommands)
}
