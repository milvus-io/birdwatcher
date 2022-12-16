package states

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	metaPath = `meta`
)

func pingEtcd(ctx context.Context, cli *clientv3.Client) error {
	_, err := cli.Get(ctx, "ping")
	return err
}

// getConnectCommand returns the command for connect etcd.
// usage: connect --etcd [address] --rootPath [rootPath]
func getConnectCommand(state State) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connect [options]",
		Short: "Connect to etcd instance",
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdAddr, err := cmd.Flags().GetString("etcd")
			if err != nil {
				return err
			}
			rootPath, err := cmd.Flags().GetString("rootPath")
			if err != nil {
				return err
			}
			metaPath, err := cmd.Flags().GetString("metaPath")
			if err != nil {
				return err
			}
			dry, err := cmd.Flags().GetBool("dry")
			if err != nil {
				return err
			}

			etcdCli, err := clientv3.New(clientv3.Config{
				Endpoints:   []string{etcdAddr},
				DialTimeout: time.Second * 10,

				// disable grpc logging
				Logger: zap.NewNop(),
			})
			if err != nil {
				return err
			}

			// ping etcd
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			err = pingEtcd(ctx, etcdCli)
			if err != nil {
				fmt.Println("cannot connect to etcd with addr:", etcdAddr, err.Error())
				return nil
			}

			fmt.Println("Using meta path:", fmt.Sprintf("%s/%s/", rootPath, metaPath))

			etcdState := getEtcdConnectedState(etcdCli, etcdAddr)

			if !dry {
				// use rootPath as instanceName
				state.SetNext(getInstanceState(etcdCli, rootPath, etcdState))
			} else {
				fmt.Println("using dry mode, ignore rootPath and metaPath")
				// rootPath empty fall back to etcd connected state
				state.SetNext(etcdState)
			}
			return nil
		},
	}
	cmd.Flags().String("etcd", "127.0.0.1:2379", "the etcd endpoint to connect")
	cmd.Flags().String("rootPath", "by-dev", "meta root path milvus is using")
	cmd.Flags().String("metaPath", metaPath, "meta path prefix")
	cmd.Flags().Bool("dry", false, "dry connect without specify milvus instance")
	return cmd
}

// findMilvusInstance iterate all possible rootPath
func findMilvusInstance(cli *clientv3.Client) ([]string, error) {
	var apps []string
	current := ""
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		resp, err := cli.Get(ctx, current, clientv3.WithKeysOnly(), clientv3.WithLimit(1), clientv3.WithFromKey())

		if err != nil {
			return nil, err
		}
		for _, kv := range resp.Kvs {
			key := string(kv.Key)
			parts := strings.Split(key, "/")
			if parts[0] != "" {
				apps = append(apps, parts[0])
			}
			// next key, since '0' is the next ascii char of '/'
			current = parts[0] + "0"
		}

		if !resp.More {
			break
		}
	}

	return apps, nil
}

func getFindMilvusCmd(cli *clientv3.Client, state *etcdConnectedState) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "find-milvus",
		Short: "search etcd kvs to find milvus instance",
		Run: func(cmd *cobra.Command, args []string) {
			apps, err := findMilvusInstance(cli)
			if err != nil {
				fmt.Println("failed to find milvus instance:", err.Error())
				return
			}
			fmt.Printf("%d candidates found:\n", len(apps))
			for _, app := range apps {
				fmt.Println(app)
			}
			state.candidates = apps
		},
	}

	return cmd
}

func getUseCmd(cli *clientv3.Client, state State) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "use [instance name]",
		Short: "use specified milvus instance",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				fmt.Println("instance name not provided")
				cmd.Usage()
				return
			}
			metaPath, err := cmd.Flags().GetString("metaPath")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			rootPath := args[0]

			fmt.Printf("Using meta path: %s/%s/\n", rootPath, metaPath)

			state.SetNext(getInstanceState(cli, rootPath, state))
		},
	}
	cmd.Flags().String("metaPath", metaPath, "meta path prefix")
	return cmd
}

type etcdConnectedState struct {
	cmdState
	client     *clientv3.Client
	addr       string
	candidates []string
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *etcdConnectedState) SetupCommands() {
	cmd := &cobra.Command{}

	cmd.AddCommand(
		// find-milvus
		getFindMilvusCmd(s.client, s),
		// use
		getUseCmd(s.client, s),
		// disconnect
		getDisconnectCmd(s),
		// exit
		getExitCmd(s),
	)

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
}

// TBD for testing only
func getEtcdConnectedState(cli *clientv3.Client, addr string) State {

	state := &etcdConnectedState{
		cmdState: cmdState{
			label: fmt.Sprintf("Etcd(%s)", addr),
		},
		client: cli,
		addr:   addr,
	}

	state.SetupCommands()

	return state
}

func (s *etcdConnectedState) Close() {
	s.client.Close()
}
