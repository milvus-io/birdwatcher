package mgrpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

type rootCoordState struct {
	*framework.CmdState
	session   *models.Session
	client    rootcoordpb.RootCoordClient
	conn      *grpc.ClientConn
	prevState framework.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *rootCoordState) SetupCommands() {
	cmd := s.GetCmd()
	s.UpdateState(cmd, s, s.SetupCommands)
}

type TestParam struct {
	framework.ParamBase `use:"pr"`
}

func (s *rootCoordState) TestCommand(ctx context.Context, p *TestParam) error {
	fmt.Println("rootcoord test")
	return nil
}

func GetRootCoordState(client rootcoordpb.RootCoordClient, conn *grpc.ClientConn, prev *framework.CmdState, session *models.Session) framework.State {
	state := &rootCoordState{
		session:   session,
		CmdState:  prev.Spawn(fmt.Sprintf("RootCoord-%d(%s)", session.ServerID, session.Address)),
		client:    client,
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}
