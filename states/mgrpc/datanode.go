package mgrpc

import (
	"fmt"

	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type dataNodeState struct {
	*framework.CmdState
	session   *models.Session
	client    datapb.DataNodeClient
	conn      *grpc.ClientConn
	prevState framework.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *dataNodeState) SetupCommands() {
	cmd := s.GetCmd()

	s.UpdateState(cmd, s, s.SetupCommands)
}

func GetDataNodeState(client datapb.DataNodeClient, conn *grpc.ClientConn, prev *framework.CmdState, session *models.Session) framework.State {
	state := &dataNodeState{
		CmdState:  prev.Spawn(fmt.Sprintf("DataNode-%d(%s)", session.ServerID, session.Address)),
		session:   session,
		client:    client,
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}
