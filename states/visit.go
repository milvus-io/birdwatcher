package states

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

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

func connectSession(session *models.Session) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
	if err != nil {
		fmt.Printf("failed to connect to %s(%d) addr: %s, err: %s\n", session.ServerName, session.ServerID, session.Address, err.Error())
	}
	return conn, err
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
		if id == session.ServerID && sessionMatch(session, sessionType) {
			conn, err = connectSession(session)
			return session, conn, err
		}
	}

	fmt.Printf("%s id:%d not found\n", sessionType, id)
	return nil, nil, errors.New("invalid id")
}

// getCoordSessionAuto automatically selects the primary coordinator session for the given type.
// When multiple coordinators exist, it picks the one whose key marks it as main/primary.
func getCoordSessionAuto(cli kv.MetaKV, basePath string, sessionType string) (session *models.Session, conn *grpc.ClientConn, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sessions, err := common.ListSessions(ctx, cli, basePath)
	if err != nil {
		fmt.Println("failed to list session, err:", err.Error())
		return nil, nil, err
	}

	var matched []*models.Session
	for _, s := range sessions {
		if sessionMatch(s, sessionType) {
			matched = append(matched, s)
		}
	}

	if len(matched) == 0 {
		fmt.Printf("no %s session found\n", sessionType)
		return nil, nil, errors.New("no session found")
	}

	if len(matched) == 1 {
		session = matched[0]
		fmt.Printf("auto-select %s, serverID: %d, addr: %s\n", sessionType, session.ServerID, session.Address)
		conn, err = connectSession(session)
		return session, conn, err
	}

	// multiple coordinators found, pick the primary/main one
	fmt.Printf("found %d %s sessions, selecting primary...\n", len(matched), sessionType)
	for _, s := range matched {
		if s.IsMain(sessionType) {
			session = s
			break
		}
	}
	if session == nil {
		// fallback to the first one if no primary found
		session = matched[0]
		fmt.Printf("no primary %s found, fallback to serverID: %d, addr: %s\n", sessionType, session.ServerID, session.Address)
	} else {
		fmt.Printf("auto-select primary %s, serverID: %d, addr: %s\n", sessionType, session.ServerID, session.Address)
	}

	conn, err = connectSession(session)
	return session, conn, err
}

// sessionMatch is the util func handles mixcoord & XXXXcoord match logic
func sessionMatch(session *models.Session, sessionType string) bool {
	if session.ServerName == sessionType {
		return true
	}
	if strings.HasSuffix(sessionType, "coord") {
		return session.ServerName == "mixcoord"
	}
	return false
}

func getVisitSessionCmds(state *framework.CmdState, cli kv.MetaKV, basePath string) []*cobra.Command {
	sessionCmds := make([]*cobra.Command, 0, len(getSessionTypes()))
	sessionTypes := getSessionTypes()

	isCoordType := func(sessionType string) bool {
		return strings.HasSuffix(sessionType, "coord")
	}

	RunFuncFactory := func(sessionType string) func(cmd *cobra.Command, args []string) {
		return func(cmd *cobra.Command, args []string) {
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

			if len(args) < 1 {
				// for coordinator types, auto-select when no server ID provided
				if isCoordType(sessionType) {
					session, conn, err := getCoordSessionAuto(cli, basePath, sessionType)
					if err != nil {
						return
					}
					setNextState(sessionType, conn, state, session)
					return
				}
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
			setNextState(sessionType, conn, state, session)
		}
	}

	for i := 0; i < len(sessionTypes); i++ {
		use := sessionTypes[i] + " {serverId}"
		if isCoordType(sessionTypes[i]) {
			use = sessionTypes[i] + " [{serverId}]"
		}
		callCmd := &cobra.Command{
			Use:   use,
			Short: "component of " + sessionTypes[i] + "s",
			Run:   RunFuncFactory(sessionTypes[i]),
		}
		callCmd.Flags().String("addr", "", "manual specified grpc addr")
		callCmd.Flags().String("sessionType", "", "")
		sessionCmds = append(sessionCmds, callCmd)
	}
	return sessionCmds
}
