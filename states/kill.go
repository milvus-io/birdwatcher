package states

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// getEtcdKillCmd returns command for kill component session
// usage: kill component
func getEtcdKillCmd(cli kv.MetaKV, basePath string) *cobra.Command {
	component := compAll
	cmd := &cobra.Command{
		Use:   "kill",
		Short: "Kill component session from etcd",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := cmd.Flags().GetInt64("id")
			if err != nil {
				return err
			}
			switch component {
			case compQueryCoord, compDataCoord, compIndexCoord, compRootCoord:
				return etcdKillComponent(cli, path.Join(basePath, "session", strings.ToLower(string(component))), id)
			case compQueryNode:
				return etcdKillComponent(cli, path.Join(basePath, "session", fmt.Sprintf("%s-%d", strings.ToLower(string(component)), id)), id)
			case compAll:
				fallthrough
			default:
				return errors.New("need to specify component type for killing")
			}
		},
	}

	cmd.Flags().Var(&component, "component", "component type to kill")
	cmd.Flags().Int64("id", 0, "Server ID to kill")
	return cmd
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
