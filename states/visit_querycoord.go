package states

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
)

type queryCoordState struct {
	*framework.CmdState
	session   *models.Session
	client    querypb.QueryCoordClient
	clientv2  querypbv2.QueryCoordClient
	conn      *grpc.ClientConn
	prevState framework.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *queryCoordState) SetupCommands() {
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

type BalanceSegmentParam struct {
	framework.ParamBase `use:"balance-segment" desc:"balance segment"`
	CollectionID        int64   `name:"collection" default:"0"`
	SegmentIDs          []int64 `name:"segment" desc:"segment ids to balance"`
	SourceNodes         []int64 `name:"srcNodes" desc:"from querynode ids"`
	DstNodes            int64   `name:"dstNode" desc:"to querynode ids"`
}

func (s *queryCoordState) BalanceSegmentCommand(ctx context.Context, p *BalanceSegmentParam) error {
	req := &querypbv2.LoadBalanceRequest{
		Base: &commonpbv2.MsgBase{
			TargetID: s.session.ServerID,
		},
		CollectionID:     p.CollectionID,
		SealedSegmentIDs: p.SegmentIDs,
		SourceNodeIDs:    p.SourceNodes,
	}

	resp, err := s.clientv2.LoadBalance(ctx, req)
	if err != nil {
		return err
	}
	fmt.Println(resp)
	return nil
}

/*
func (s *queryCoordState) ShowCollectionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "show-collection",
		Run: func(cmd *cobra.Command, args []string) {
			collection, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				cmd.Usage()
				return
			}

			req := &querypbv2.ShowCollectionsRequest{
				Base: &commonpbv2.MsgBase{
					TargetID: s.session.ServerID,
				},
				CollectionIDs: []int64{collection},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			resp, err := s.clientv2.ShowCollections(ctx, req)
			if err != nil {
				fmt.Println(err.Error())
			}

			fmt.Printf("%s, %s", resp.GetStatus().GetErrorCode().String(), resp.GetStatus().GetReason())
		},
	}

	cmd.Flags().Int64("collection", 0, "collection to show")
	return cmd
}*/

func getQueryCoordState(client querypb.QueryCoordClient, conn *grpc.ClientConn, prev framework.State, session *models.Session) framework.State {
	state := &queryCoordState{
		CmdState:  framework.NewCmdState(fmt.Sprintf("QueryCoord-%d(%s)", session.ServerID, session.Address)),
		session:   session,
		client:    client,
		clientv2:  querypbv2.NewQueryCoordClient(conn),
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}

func checkerActivationCmd(clientv2 querypbv2.QueryCoordClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "checker",
		Short: "checker cmd",
	}
	cmd.AddCommand(
		checkerActivateCmd(clientv2, id),
		checkerDeactivateCmd(clientv2, id),
		checkerListCmd(clientv2, id),
	)
	return cmd
}

func checkerActivateCmd(clientv2 querypbv2.QueryCoordClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "activate",
		Short: "activate checkerID",
		Run: func(cmd *cobra.Command, args []string) {
			checkerID, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				fmt.Println("checkerID must be a number")
				return
			}
			req := &querypbv2.ActivateCheckerRequest{
				Base: &commonpbv2.MsgBase{
					TargetID: id,
					SourceID: -1,
				},
				CheckerID: int32(checkerID),
			}

			status, err := clientv2.ActivateChecker(context.Background(), req)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if status.ErrorCode != commonpbv2.ErrorCode_Success {
				fmt.Print(status.Reason)
				return
			}
			fmt.Println("success")
		},
	}

	return cmd
}

func checkerDeactivateCmd(clientv2 querypbv2.QueryCoordClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "deactivate",
		Short: "deactivate checkerID",
		Run: func(cmd *cobra.Command, args []string) {
			checkerID, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				fmt.Println("checkerID must be a number")
				return
			}
			req := &querypbv2.DeactivateCheckerRequest{
				Base: &commonpbv2.MsgBase{
					TargetID: id,
					SourceID: -1,
				},
				CheckerID: int32(checkerID),
			}

			status, err := clientv2.DeactivateChecker(context.Background(), req)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if status.ErrorCode != commonpbv2.ErrorCode_Success {
				fmt.Print(status.Reason)
				return
			}
			fmt.Println("success")
		},
	}

	return cmd
}

func checkerListCmd(clientv2 querypbv2.QueryCoordClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "checker list [checkerIDs]",
		Run: func(cmd *cobra.Command, args []string) {
			checkerIDs := make([]int32, 0)
			for _, arg := range args {
				checkerID, err := strconv.ParseInt(arg, 10, 32)
				if err != nil {
					fmt.Println("checkerID must be number")
				}
				checkerIDs = append(checkerIDs, int32(checkerID))
			}

			req := &querypbv2.ListCheckersRequest{
				Base: &commonpbv2.MsgBase{
					TargetID: id,
					SourceID: -1,
				},
				CheckerIDs: checkerIDs,
			}

			resp, err := clientv2.ListCheckers(context.Background(), req)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if resp.Status.ErrorCode != commonpbv2.ErrorCode_Success {
				fmt.Println(resp.Status.Reason)
				return
			}

			sort.Slice(resp.CheckerInfos, func(i, j int) bool {
				return resp.CheckerInfos[i].GetId() < resp.CheckerInfos[j].GetId()
			})
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.AlignRight|tabwriter.Debug)
			fmt.Fprintln(w, "id\tdesc\tfound\tactivated")
			for _, info := range resp.CheckerInfos {
				fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", info.GetId(), info.GetDesc(), info.GetFound(), info.GetActivated())
			}
			w.Flush()
		},
	}
	return cmd
}
