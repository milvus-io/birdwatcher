package states

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/cockroachdb/errors"
)

type openParam struct {
	ParamBase     `use:"open-workspace [workspace-name]" desc:"Open workspace"`
	workspaceName string
}

// ParseArgs parse args
func (p *openParam) ParseArgs(args []string) error {
	if len(args) == 0 {
		return errors.New("no backup file provided")
	}
	if len(args) > 1 {
		return errors.New("only one backup file is allowed")
	}
	p.workspaceName = args[0]
	return nil
}

// OpenCommand implements open workspace command
func (s *disconnectState) OpenCommand(ctx context.Context, p *openParam) error {
	workspaceName := p.workspaceName
	workPath := path.Join(s.config.WorkspacePath, workspaceName)
	info, err := os.Stat(workPath)
	if os.IsNotExist(err) {
		fmt.Printf("workspace %s not exist\n", workspaceName)
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("workspace %s is not a directory", workspaceName)
	}

	server, err := startEmbedEtcdServer(workPath, true)
	if err != nil {
		return fmt.Errorf("failed to start embed etcd server in workspace %s, err: %s", workspaceName, err.Error())
	}

	nextState := getEmbedEtcdInstanceV2(server, s.config)
	err = nextState.setupWorkDir(workPath)
	if err != nil {
		return fmt.Errorf("failed to setup workspace for %s, err: %s", workspaceName, err.Error())
	}

	s.SetNext(nextState)
	return nil
}
