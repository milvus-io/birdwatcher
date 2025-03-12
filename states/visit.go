package states

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/birdwatcher/states/mgrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

func getSessionTypes() []string {
	return []string{
		"datacoord",
		"datanode",
		"indexcoord",
		"indexnode",
		"querycoord",
		"querynode",
		"rootcoord",
	}
}

func getVisitCmd(state *framework.CmdState, cli kv.MetaKV, basePath string) *cobra.Command {
	callCmd := &cobra.Command{
		Use:   "visit",
		Short: "enter state that could visit some service of component",
	}

	callCmd.AddCommand(
		getVisitSessionCmds(state, cli, basePath)...,
	)

	return callCmd
}

func setNextState(sessionType string, conn *grpc.ClientConn, state *framework.CmdState, session *models.Session) {
	switch sessionType {
	case "datacoord":
		client := datapb.NewDataCoordClient(conn)
		state.SetNext("dc", mgrpc.GetDataCoordState(client, conn, state, session))
	case "datanode":
		client := datapb.NewDataNodeClient(conn)
		state.SetNext("dn", mgrpc.GetDataNodeState(client, conn, state, session))
	case "indexcoord":
		client := indexpb.NewIndexCoordClient(conn)
		state.SetNext("ic", mgrpc.GetIndexCoordState(client, conn, state, session))
	case "indexnode":
		// client := indexpb.NewIndexNodeClient(conn)
		// state.SetNext("in", getIndexNodeState(client, conn, state, session))
	case "querycoord":
		client := querypb.NewQueryCoordClient(conn)
		state.SetNext("qc", mgrpc.GetQueryCoordState(client, conn, state, session))
	case "querynode":
		client := querypb.NewQueryNodeClient(conn)
		state.SetNext("qn", mgrpc.GetQueryNodeState(client, conn, state, session))
	case "rootcoord":
		client := rootcoordpb.NewRootCoordClient(conn)
		state.SetNext("rc", mgrpc.GetRootCoordState(client, conn, state, session))
	}
}

func getSessionConnect(cli kv.MetaKV, basePath string, id int64, sessionType string) (session *models.Session, conn *grpc.ClientConn, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sessions, err := common.ListSessions(ctx, cli, basePath)
	if err != nil {
		fmt.Println("failed to list session, err:", err.Error())
		return nil, nil, err
	}

	for _, session := range sessions {
		if id == session.ServerID && session.ServerName == sessionType {
			opts := []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			}

			conn, err = grpc.DialContext(context.Background(), session.Address, opts...)
			if err != nil {
				fmt.Printf("failed to connect to proxy(%d) addr: %s, err: %s\n", session.ServerID, session.Address, err.Error())
			}
			return session, conn, err
		}
	}

	fmt.Printf("%s id:%d not found\n", sessionType, id)
	return nil, nil, errors.New("invalid id")
}

func getVisitSessionCmds(state *framework.CmdState, cli kv.MetaKV, basePath string) []*cobra.Command {
	sessionCmds := make([]*cobra.Command, 0, len(getSessionTypes()))
	sessionTypes := getSessionTypes()

	RunFuncFactory := func(sessionType string) func(cmd *cobra.Command, args []string) {
		return func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				cmd.Usage()
				return
			}
			id, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				cmd.Usage()
				return
			}
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				cmd.Usage()
				return
			}
			sType, err := cmd.Flags().GetString("sessionType")
			if err != nil {
				cmd.Usage()
				return
			}
			if addr != "" && sType != "" {
				opts := []grpc.DialOption{
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithBlock(),
				}

				conn, err := grpc.DialContext(context.Background(), addr, opts...)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				setNextState(sessionType, conn, state, &models.Session{
					Address: addr,
				})
				return
			}
			session, conn, err := getSessionConnect(cli, basePath, id, sessionType)
			if err != nil {
				return
			}
			setNextState(sessionType, conn, state, session)
		}
	}

	for i := 0; i < len(sessionTypes); i++ {
		callCmd := &cobra.Command{
			Use:   sessionTypes[i] + " {serverId}",
			Short: "component of " + sessionTypes[i] + "s",
			Run:   RunFuncFactory(sessionTypes[i]),
		}
		callCmd.Flags().String("addr", "", "manual specified grpc addr")
		callCmd.Flags().String("sessionType", "", "")
		sessionCmds = append(sessionCmds, callCmd)
	}
	return sessionCmds
}
