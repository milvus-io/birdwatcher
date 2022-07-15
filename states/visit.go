package states

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/congqixia/birdwatcher/proto/v2.0/querypb"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func getVisitCmd(state State, cli *clientv3.Client, basePath string) *cobra.Command {
	callCmd := &cobra.Command{
		Use:   "visit",
		Short: "enter state that could visit some service of component",
	}

	callCmd.AddCommand(
		getVisitQueryNodeCmd(state, cli, basePath),
	)

	return callCmd
}

func getVisitQueryNodeCmd(state State, cli *clientv3.Client, basePath string) *cobra.Command {
	callQNCmd := &cobra.Command{
		Use:   "querynode {nodeid}",
		Short: "components of querynodes",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				cmd.Usage()
				return
			}
			id, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				cmd.Usage()
				return
			}
			sessions, err := listSessions(cli, basePath)
			if err != nil {
				fmt.Println("failed to list session, err:", err.Error())
				return
			}

			for _, session := range sessions {
				if id == session.ServerID && session.ServerName == "querynode" {
					//client :=
					opts := []grpc.DialOption{
						grpc.WithInsecure(),
						grpc.WithBlock(),
						grpc.WithTimeout(2 * time.Second),
					}

					conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
					if err != nil {
						fmt.Printf("failed to connect to proxy(%d) addr: %s, err: %s\n", id, session.Address, err.Error())
					}

					client := querypb.NewQueryNodeClient(conn)
					state.SetNext(getQueryNodeState(client, conn, state, session))
					return
				}
			}

			fmt.Printf("querynode id:%d not found\n", id)
		},
	}

	return callQNCmd
}
