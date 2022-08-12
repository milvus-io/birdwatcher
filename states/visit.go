package states

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/indexpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/rootcoordpb"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
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

func getVisitCmd(state State, cli *clientv3.Client, basePath string) *cobra.Command {
	callCmd := &cobra.Command{
		Use:   "visit",
		Short: "enter state that could visit some service of component",
	}

	callCmd.AddCommand(
		getVisitSessionCmds(state, cli, basePath)...,
	)

	return callCmd
}

func setNextState(sessionType string, conn *grpc.ClientConn, statePtr *State, session *models.Session) {
	state := *statePtr
	switch sessionType {
	case "datacoord":
		client := datapb.NewDataCoordClient(conn)
		state.SetNext(getDataCoordState(client, conn, state, session))
	case "datanode":
		client := datapb.NewDataNodeClient(conn)
		state.SetNext(getDataNodeState(client, conn, state, session))
	case "indexcoord":
		client := indexpb.NewIndexCoordClient(conn)
		state.SetNext(getIndexCoordState(client, conn, state, session))
	case "indexnode":
		client := indexpb.NewIndexNodeClient(conn)
		state.SetNext(getIndexNodeState(client, conn, state, session))
	case "querycoord":
		client := querypb.NewQueryCoordClient(conn)
		state.SetNext(getQueryCoordState(client, conn, state, session))
	case "querynode":
		client := querypb.NewQueryNodeClient(conn)
		state.SetNext(getQueryNodeState(client, conn, state, session))
	case "rootcoord":
		client := rootcoordpb.NewRootCoordClient(conn)
		state.SetNext(getRootCoordState(client, conn, state, session))
	}
}

func getSessionConnect(cli *clientv3.Client, basePath string, id int64, sessionType string) (session *models.Session, conn *grpc.ClientConn, err error) {
	sessions, err := listSessions(cli, basePath)
	if err != nil {
		fmt.Println("failed to list session, err:", err.Error())
		return nil, nil, err
	}

	for _, session := range sessions {
		if id == session.ServerID && session.ServerName == sessionType {
			opts := []grpc.DialOption{
				grpc.WithInsecure(),
				grpc.WithBlock(),
				grpc.WithTimeout(2 * time.Second),
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
func getVisitSessionCmds(state State, cli *clientv3.Client, basePath string) []*cobra.Command {
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
			session, conn, err := getSessionConnect(cli, basePath, id, sessionType)
			if err != nil {
				return
			}
			setNextState(sessionType, conn, &state, session)
		}
	}

	for i := 0; i < len(sessionTypes); i++ {
		callCmd := &cobra.Command{
			Use:   sessionTypes[i] + " {serverId}",
			Short: "component of " + sessionTypes[i] + "s",
			Run:   RunFuncFactory(sessionTypes[i]),
		}
		sessionCmds = append(sessionCmds, callCmd)
	}
	return sessionCmds
}
