package states

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	metaPath = `meta`
)

var (
	// ErrNotMilvsuRootPath sample error for non-valid root path.
	ErrNotMilvsuRootPath = errors.New("is not a Milvus RootPath")
)

func pingEtcd(ctx context.Context, cli clientv3.KV, rootPath string, metaPath string) error {
	key := path.Join(rootPath, metaPath, "session/id")
	resp, err := cli.Get(ctx, key)
	if err != nil {
		return err
	}

	if len(resp.Kvs) == 0 {
		return fmt.Errorf("\"%s\" %w", rootPath, ErrNotMilvsuRootPath)
	}
	return nil
}

type ConnectParams struct {
	framework.ParamBase `use:"connect" desc:"Connect to etcd"`
	EtcdAddr            string `name:"etcd" default:"127.0.0.1:2379" desc:"the etcd endpoint to connect"`
	RootPath            string `name:"rootPath" default:"by-dev" desc:"meta root paht milvus is using"`
	MetaPath            string `name:"metaPath" default:"meta" desc:"meta path prefix"`
	Force               bool   `name:"force" default:"false" desc:"force connect ignoring ping Etcd & rootPath check"`
	Dry                 bool   `name:"dry" default:"false" desc:"dry connect without specifying milvus instance"`
}

func (s *disconnectState) ConnectCommand(ctx context.Context, cp *ConnectParams) error {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cp.EtcdAddr},
		DialTimeout: time.Second * 10,

		// disable grpc logging
		Logger: zap.NewNop(),
	})
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	etcdState := getEtcdConnectedState(etcdCli, cp.EtcdAddr, s.config)
	if !cp.Dry {
		// ping etcd
		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		err = pingEtcd(ctx, etcdCli, cp.RootPath, cp.MetaPath)
		if err != nil {
			if errors.Is(err, ErrNotMilvsuRootPath) {
				if !cp.Force {
					etcdCli.Close()
					fmt.Printf("Connection established, but %s, please check your config or use Dry mode\n", err.Error())
					return err
				}
			} else {
				fmt.Println("cannot connect to etcd with addr:", cp.EtcdAddr, err.Error())
				return err
			}
		}

		fmt.Println("Using meta path:", fmt.Sprintf("%s/%s/", cp.RootPath, metaPath))

		// use rootPath as instanceName
		s.SetNext(getInstanceState(etcdCli, cp.RootPath, cp.MetaPath, etcdState, s.config))
	} else {
		fmt.Println("using dry mode, ignore rootPath and metaPath")
		// rootPath empty fall back to etcd connected state
		s.SetNext(etcdState)
	}
	return nil
}

/*
func getUseCmd(cli clientv3.KV, state State, config *configs.Config) *cobra.Command {
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
			force, err := cmd.Flags().GetBool("force")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			rootPath := args[0]

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err = pingEtcd(ctx, cli, rootPath, metaPath)
			if err != nil {
				if errors.Is(err, ErrNotMilvsuRootPath) {
					if !force {
						fmt.Printf("Connection established, but %s, please check your config or use Dry mode\n", err.Error())
						return
					}
				} else {
					fmt.Println("failed to ping etcd", err.Error())
					return
				}
			}

			fmt.Printf("Using meta path: %s/%s/\n", rootPath, metaPath)

			state.SetNext(getInstanceState(cli, rootPath, state, config))
		},
	}
	cmd.Flags().Bool("force", false, "force connect ignoring ping Etcd rootPath check")
	cmd.Flags().String("metaPath", metaPath, "meta path prefix")
	return cmd
}*/

type etcdConnectedState struct {
	cmdState
	client     *clientv3.Client
	addr       string
	candidates []string
	config     *configs.Config
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *etcdConnectedState) SetupCommands() {
	cmd := &cobra.Command{}

	s.mergeFunctionCommands(cmd, s)

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
}

// getEtcdConnectedState returns etcdConnectedState for unknown instance
func getEtcdConnectedState(cli *clientv3.Client, addr string, config *configs.Config) State {

	state := &etcdConnectedState{
		cmdState: cmdState{
			label: fmt.Sprintf("Etcd(%s)", addr),
		},
		client: cli,
		addr:   addr,
		config: config,
	}

	state.SetupCommands()

	return state
}

func (s *etcdConnectedState) DisconnectCommand(ctx context.Context, p *DisconnectParam) error {
	s.SetNext(Start(s.config))
	s.Close()
	return nil
}

type FindMilvusParam struct {
	framework.ParamBase `use:"find-milvus" desc:"search etcd kvs to find milvus instance"`
}

func (s *etcdConnectedState) FindMilvusCommand(ctx context.Context, p *FindMilvusParam) error {
	apps, err := findMilvusInstance(ctx, s.client)
	if err != nil {
		return err
	}
	fmt.Printf("%d candidates found:\n", len(apps))
	for _, app := range apps {
		fmt.Println(app)
	}
	s.candidates = apps
	return nil
}

type UseParam struct {
	framework.ParamBase `use:"use [instance-name]" desc:"use specified milvus instance"`
	instanceName        string
	Force               bool   `name:"force" default:"false" desc:"force connect ignoring ping result"`
	MetaPath            string `name:"metaPath" default:"meta" desc:"meta path prefix"`
}

func (p *UseParam) ParseArgs(args []string) error {
	if len(args) != 1 {
		return errors.New("instance shall be provided")
	}
	p.instanceName = args[0]
	return nil
}

func (s *etcdConnectedState) UseCommand(ctx context.Context, p *UseParam) error {
	err := pingEtcd(ctx, s.client, p.instanceName, p.MetaPath)
	if err != nil {
		if errors.Is(err, ErrNotMilvsuRootPath) {
			if !p.Force {
				fmt.Printf("Connection established, but %s, please check your config or use Dry mode\n", err.Error())
				return err
			}
		} else {
			fmt.Println("failed to ping etcd", err.Error())
			return err
		}
	}

	fmt.Printf("Using meta path: %s/%s/\n", p.instanceName, p.MetaPath)

	s.SetNext(getInstanceState(s.client, p.instanceName, p.MetaPath, s, s.config))
	return nil
}

// findMilvusInstance iterate all possible rootPath
func findMilvusInstance(ctx context.Context, cli clientv3.KV) ([]string, error) {
	var apps []string
	current := ""
	for {
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

func (s *etcdConnectedState) Close() {
	s.client.Close()
}
