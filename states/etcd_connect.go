package states

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
)

const (
	metaPath = `meta`
)

// ErrNotMilvsuRootPath sample error for non-valid root path.
var ErrNotMilvsuRootPath = errors.New("is not a Milvus RootPath")

func pingInstance(ctx context.Context, cli clientv3.KV, rootPath string, metaPath string) error {
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
	EnableTLS           bool   `name:"enableTLS" default:"false" desc:"use TLS"`
	RootCA              string `name:"rootCAPem" default:"" desc:"root CA pem file path"`
	ETCDPem             string `name:"etcdCert" default:"" desc:"etcd tls cert file path"`
	ETCDKey             string `name:"etcdKey" default:"" desc:"etcd tls key file path"`
	Auto                bool   `name:"auto" default:"false" desc:"auto detect rootPath if possible"`
}

func (s *disconnectState) getTLSConfig(cp *ConnectParams) (*tls.Config, error) {
	if !cp.EnableTLS {
		return nil, nil
	}

	rootCertPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}

	if cp.RootCA != "" {
		bs, err := os.ReadFile(cp.RootCA)
		if err != nil {
			return nil, err
		}

		ok := rootCertPool.AppendCertsFromPEM(bs)
		if !ok {
			return nil, errors.New("Root CA PEM cannot be parsed")
		}
	}

	cert, err := tls.LoadX509KeyPair(cp.ETCDPem, cp.ETCDKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load etcd cert/key pair")
	}

	return &tls.Config{
		RootCAs: rootCertPool,
		Certificates: []tls.Certificate{
			cert,
		},
		// gosec
		MinVersion: tls.VersionTLS12,
	}, nil
}

func (s *disconnectState) ConnectCommand(ctx context.Context, cp *ConnectParams) error {
	tls, err := s.getTLSConfig(cp)
	if err != nil {
		return errors.Wrap(err, "failed to load tls certificates")
	}
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cp.EtcdAddr},
		DialTimeout: time.Second * 10,
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
		},

		TLS: tls,
		// disable grpc logging
		Logger: zap.NewNop(),
	})
	if err != nil {
		return errors.Wrap(err, "failed to connect to etcd")
	}

	if cp.Auto {
		candidates, err := findMilvusInstance(ctx, etcdCli)
		if err != nil {
			return err
		}
		if len(candidates) == 1 {
			cp.RootPath = candidates[0]
		} else if len(candidates) > 1 {
			fmt.Println("multiple possible rootPath find, cannot use auto mode")
		} else {
			fmt.Println("failed to find rootPath candidate")
			return nil
		}
	}

	etcdState := getEtcdConnectedState(etcdCli, cp.EtcdAddr, s.config)
	if !cp.Dry {
		// ping etcd
		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		err = pingInstance(ctx, etcdCli, cp.RootPath, cp.MetaPath)
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
	err := pingInstance(ctx, s.client, p.instanceName, p.MetaPath)
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
