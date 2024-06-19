package states

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/indexpb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type indexCoordState struct {
	*framework.CmdState
	session   *models.Session
	client    indexpb.IndexCoordClient
	clientv2  indexpbv2.IndexCoordClient
	conn      *grpc.ClientConn
	prevState framework.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *indexCoordState) SetupCommands() {
	cmd := &cobra.Command{}
	cmd.AddCommand(
		// metrics
		getMetricsCmd(s.client),
		// configuration
		getConfigurationCmd(s.clientv2, s.session.ServerID),
		// build index progress
		getDescribeIndex(s.clientv2, s.session.ServerID),
		// back
		getBackCmd(s, s.prevState),
		// exit
		getExitCmd(s),
	)

	s.MergeFunctionCommands(cmd, s)

	s.CmdState.RootCmd = cmd
	s.SetupFn = s.SetupCommands
}

func getIndexCoordState(client indexpb.IndexCoordClient, conn *grpc.ClientConn, prev framework.State, session *models.Session) framework.State {
	state := &indexCoordState{
		CmdState:  framework.NewCmdState(fmt.Sprintf("IndexCoord-%d(%s)", session.ServerID, session.Address)),
		session:   session,
		client:    client,
		clientv2:  indexpbv2.NewIndexCoordClient(conn),
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}

func getDescribeIndex(client indexpbv2.IndexCoordClient, serverID int64) *cobra.Command {
	cmd := &cobra.Command{
		Use: "describe",
		Run: func(cmd *cobra.Command, args []string) {
			collectionID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println("empty collectionID, err:", err.Error())
				cmd.Usage()
				return
			}
			resp, err := client.DescribeIndex(context.Background(), &indexpbv2.DescribeIndexRequest{
				CollectionID: collectionID,
			})
			if err != nil {
				fmt.Println("failed to call grpc, err:", err.Error())
				return
			}
			for _, info := range resp.GetIndexInfos() {
				printIndexV2(info)
			}
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id")
	return cmd
}

func printIndexV2(index *indexpbv2.IndexInfo) {
	fmt.Println("==================================================================")
	fmt.Printf("Index ID: %d\tIndex Name: %s\tCollectionID:%d\n", index.GetIndexID(), index.GetIndexName(), index.GetCollectionID())
	fmt.Printf("Indexed Rows: %d\n", index.GetIndexedRows())
	fmt.Printf("Total Rows: %d\n", index.GetTotalRows())
	indexParams := index.GetIndexParams()
	fmt.Printf("Index Type: %s\tMetric Type: %s\n",
		common.GetKVPair(indexParams, "index_type"),
		common.GetKVPair(indexParams, "metric_type"),
	)
	fmt.Printf("Index Params: %s\n", common.GetKVPair(index.GetUserIndexParams(), "params"))
	fmt.Println("==================================================================")
}
