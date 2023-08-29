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
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
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

func (app *ApplicationState) ConnectCommand(ctx context.Context, cp *ConnectParams) error {
	tls, err := app.getTLSConfig(cp)
	if err != nil {
		return err
	}

	cfg := clientv3.Config{
		Endpoints:   []string{cp.EtcdAddr},
		DialTimeout: time.Second * 10,

		TLS: tls,
		// disable grpc logging
		Logger: zap.NewNop(),
	}
	etcdCli, err := clientv3.New(cfg)
	if err != nil {
		return err
	}

	etcdState := getEtcdConnectedState(app.core, etcdCli, cp.EtcdAddr, app.config)
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
		app.SetTagNext(etcdTag, getInstanceState(app.core, etcdCli, cp.RootPath, cp.MetaPath, etcdState, app.config))
	} else {
		fmt.Println("using dry mode, ignore rootPath and metaPath")
		// rootPath empty fall back to etcd connected state
		app.SetTagNext(etcdTag, etcdState)
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
	UseSSL              bool   `name:"use_ssl" default:"false" desc:"enable to use SSL"`
	EnableTLS           bool   `name:"enableTLS" default:"false" desc:"use TLS"`
	RootCA              string `name:"rootCAPem" default:"" desc:"root CA pem file path"`
	ETCDPem             string `name:"etcdCert" default:"" desc:"etcd tls cert file path"`
	ETCDKey             string `name:"etcdKey" default:"" desc:"etcd tls key file path"`
	TLSMinVersion       string `name:"min_version" default:"1.2" desc:"TLS min version"`
}

func (app *ApplicationState) getTLSConfig(cp *ConnectParams) (*tls.Config, error) {
	if !cp.EnableTLS {
		return nil, nil
	}

	var tlsMinVersion uint16
	switch cp.TLSMinVersion {
	case "1.0":
		tlsMinVersion = tls.VersionTLS10
	case "1.1":
		tlsMinVersion = tls.VersionTLS11
	case "1.2":
		tlsMinVersion = tls.VersionTLS12
	case "1.3":
		tlsMinVersion = tls.VersionTLS13
	default:
		return nil, errors.New("invalid min tls version, only 1.0, 1.1, 1.2 and 1.3 is supported")
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

	// #nosec G402
	return &tls.Config{
		RootCAs: rootCertPool,
		Certificates: []tls.Certificate{
			cert,
		},
		MinVersion: tlsMinVersion,
	}, nil
}

type etcdConnectedState struct {
	*framework.CmdState
	client     *clientv3.Client
	addr       string
	candidates []string
	config     *configs.Config
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *etcdConnectedState) SetupCommands() {
	cmd := s.GetCmd()

	s.UpdateState(cmd, s, s.SetupCommands)
}

// getEtcdConnectedState returns etcdConnectedState for unknown instance
func getEtcdConnectedState(parent *framework.CmdState, cli *clientv3.Client, addr string, config *configs.Config) framework.State {

	state := &etcdConnectedState{
		CmdState: parent.Spawn(fmt.Sprintf("Etcd(%s)", addr)),
		client:   cli,
		addr:     addr,
		config:   config,
	}

	return state
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

	s.SetNext(getInstanceState(s.CmdState, s.client, p.instanceName, p.MetaPath, s, s.config))
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
