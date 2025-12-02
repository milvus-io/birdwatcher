package states

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

type EtcdKillParam struct {
	framework.ParamBase `use:"kill" desc:"Kill component session from etcd"`
	Component           string `name:"component" default:"" desc:"component type to kill"`
	NodeID              int64  `name:"id" default:"0" desc:"Server ID to kill"`
}

func (s *InstanceState) KillCommand(ctx context.Context, p *EtcdKillParam) error {
	switch milvusComponent(strings.ToUpper(p.Component)) {
	case compQueryCoord, compDataCoord, compIndexCoord, compRootCoord, compMixCoord:
		return etcdKillComponent(s.client, path.Join(s.basePath, "session", strings.ToLower(string(p.Component))), p.NodeID)
	case compQueryNode, compDataNode:
		return etcdKillComponent(s.client, path.Join(s.basePath, "session", fmt.Sprintf("%s-%d", strings.ToLower(string(p.Component)), p.NodeID)), p.NodeID)
	case compAll:
		fallthrough
	default:
		return errors.New("need to specify component type for killing")
	}
}

func etcdKillComponent(cli kv.MetaKV, key string, id int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	val, err := cli.Load(ctx, key)
	if err != nil {
		return err
	}

	session := &models.Session{}

	err = json.Unmarshal([]byte(val), session)
	if err != nil {
		return fmt.Errorf("faild to parse session for key %s, error: %w", key, err)
	}

	if session.ServerID != id {
		return errors.New("session id no match")
	}

	// remove session

	return cli.Remove(context.Background(), key)
}
