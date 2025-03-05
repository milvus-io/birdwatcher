package states

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	milvuspbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb"
)

type dataCoordState struct {
	common.CmdState
	session   *models.Session
	client    datapb.DataCoordClient
	clientv2  datapbv2.DataCoordClient
	conn      *grpc.ClientConn
	prevState common.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *dataCoordState) SetupCommands() {
	cmd := &cobra.Command{}
	cmd.AddCommand(
		// metrics
		getMetricsCmd(s.client),
		// configuration
		getConfigurationCmd(s.clientv2, s.session.ServerID),
		// back
		getBackCmd(s, s.prevState),

		// exit
		getExitCmd(s),
	)

	s.MergeFunctionCommands(cmd, s)

	s.CmdState.RootCmd = cmd
	s.SetupFn = s.SetupCommands
}

type CompactParam struct {
	framework.ParamBase `use:"compact" desc:"manual compact with collectionID"`
	CollectionID        int64 `name:"collectionID" default:"0" desc:"collection id to compact"`
	CompactAll          bool  `name:"compactAll" default:"false" desc:"explicitly allow compact all collections"`
}

func (s *dataCoordState) CompactCommand(ctx context.Context, p *CompactParam) error {
	if p.CollectionID <= 0 && !p.CompactAll {
		return errors.New("collection id not provided, set `compactAll` flag if needed")
	}
	resp, err := s.clientv2.ManualCompaction(ctx, &milvuspbv2.ManualCompactionRequest{
		CollectionID: p.CollectionID,
	})
	if err != nil {
		return errors.Wrapf(err, "manual compact fail with collectionID:%d, error: %s", p.CollectionID)
	}
	fmt.Printf("manual compact done, collectionID:%d, compactionID:%d, rpc status:%v\n",
		p.CollectionID, resp.GetCompactionID(), resp.GetStatus())
	return nil
}

type FlushParam struct {
	framework.ParamBase `use:"flush" desc:"manual flush with collectionID"`
	CollectionID        int64 `name:"collectionID" default:"0" desc:"collection id to compact"`
}

func (s *dataCoordState) FlushCommand(ctx context.Context, p *FlushParam) error {
	resp, err := s.clientv2.Flush(ctx, &datapbv2.FlushRequest{
		CollectionID: p.CollectionID,
	})
	if err != nil {
		return errors.Wrapf(err, "manual compact fail with collectionID:%d, error: %s", p.CollectionID)
	}
	fmt.Printf("manual flush done, collectionID:%d, rpc status:%v\n",
		p.CollectionID, resp.GetStatus())
	return nil
}

func getDataCoordState(client datapb.DataCoordClient, conn *grpc.ClientConn, prev common.State, session *models.Session) common.State {
	state := &dataCoordState{
		CmdState: common.CmdState{
			LabelStr: fmt.Sprintf("DataCoord-%d(%s)", session.ServerID, session.Address),
		},
		session:   session,
		client:    client,
		clientv2:  datapbv2.NewDataCoordClient(conn),
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}
