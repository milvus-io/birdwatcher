package states

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

const (
	workspaceMetaFile = `.bw_project`
)

type embedEtcdMockState struct {
	cmdState
	client       *clientv3.Client
	server       *embed.Etcd
	instanceName string
	workDir      string

	metrics        map[string][]byte
	defaultMetrics map[string][]byte
	config         *configs.Config
}

// Close implements State.
// Clean up embed etcd folder content.
func (s *embedEtcdMockState) Close() {
	if s.client != nil {
		s.client.Close()
	}
	if s.server != nil {
		s.server.Close()
		os.RemoveAll(s.server.Config().Dir)
	}
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *embedEtcdMockState) SetupCommands() {
	cmd := &cobra.Command{}

	rootPath := path.Join(s.instanceName, metaPath)

	cmd.AddCommand(
		// show [subcommand] options...
		etcd.ShowCommand(s.client, rootPath),

		// remove [subcommand] options...
		// used for testing
		etcd.RemoveCommand(s.client, rootPath),
		// download-pk
		getDownloadPKCmd(s.client, rootPath),
		// inspect-pk
		getInspectPKCmd(s.client, rootPath),

		// force-release
		getForceReleaseCmd(s.client, rootPath),

		// disconnect
		getDisconnectCmd(s, s.config),

		// for testing
		etcd.RepairCommand(s.client, rootPath),

		getPrintMetricsCmd(s),

		// exit
		getExitCmd(s),
	)
	cmd.AddCommand(getGlobalUtilCommands()...)
	cmd.AddCommand(etcd.RawCommands(s.client)...)

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
}

func (s *embedEtcdMockState) SetInstance(instanceName string) {
	s.cmdState.label = fmt.Sprintf("Backup(%s)", instanceName)
	s.instanceName = instanceName
	s.SetupCommands()
}

func (s *embedEtcdMockState) setupWorkDir(dir string) error {
	s.workDir = dir
	return s.syncWorkspaceInfo()
}

// syncWorkspaceInfo try to read pre-written workspace meta info.
// if not exist or version is older, write a new one.
func (s *embedEtcdMockState) syncWorkspaceInfo() error {
	metaFilePath := path.Join(s.workDir, workspaceMetaFile)
	err := testFile(metaFilePath)
	if err != nil {
		switch {
		case os.IsNotExist(err):
			fmt.Printf("%s not exist, writing a new one", metaFilePath)
			// meta file not exist
			s.writeWorkspaceMeta(metaFilePath)
		case errors.Is(err, ErrPathIsDir):
			fmt.Printf("%s is a directory, init workspace failed\n", metaFilePath)
			return err
		default:
			fmt.Printf("%s cannot setup as workspace meta, err: %s\n", metaFilePath, err.Error())
			return err
		}
	}

	s.readWorkspaceMeta(metaFilePath)
	return nil
}

func (s *embedEtcdMockState) writeWorkspaceMeta(path string) {
	file, err := os.Create(path)
	if err != nil {
		fmt.Println("failed to open meta file to write", err.Error())
		return
	}
	defer file.Close()

	meta := &models.WorkspaceMeta{
		Version:  "0.0.1",
		Instance: s.instanceName,
		MetaPath: metaPath,
	}

	bs, err := proto.Marshal(meta)
	if err != nil {
		fmt.Println("failed to marshal meta info", err.Error())
		return
	}
	r := bufio.NewWriter(file)

	writeBackupBytes(r, bs)
	r.Flush()
}

func (s *embedEtcdMockState) readWorkspaceMeta(path string) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("failed to open meta file %s to read, err: %s\n", path, err.Error())
		return
	}
	defer file.Close()

	r := bufio.NewReader(file)

	meta := models.WorkspaceMeta{}
	err = readFixLengthHeader(r, &meta)
	if err != nil {
		fmt.Printf("failed to read %s as meta file, %s\n", path, err.Error())
		return
	}

	s.SetInstance(meta.Instance)
}

func getEmbedEtcdInstance(server *embed.Etcd, cli *clientv3.Client, instanceName string, config *configs.Config) State {

	state := &embedEtcdMockState{
		cmdState: cmdState{
			label: fmt.Sprintf("Backup(%s)", instanceName),
		},
		instanceName:   instanceName,
		server:         server,
		client:         cli,
		metrics:        make(map[string][]byte),
		defaultMetrics: make(map[string][]byte),
		config:         config,
	}

	state.SetupCommands()

	return state
}

func getEmbedEtcdInstanceV2(server *embed.Etcd, config *configs.Config) *embedEtcdMockState {

	client := v3client.New(server.Server)
	state := &embedEtcdMockState{
		cmdState:       cmdState{},
		server:         server,
		client:         client,
		metrics:        make(map[string][]byte),
		defaultMetrics: make(map[string][]byte),
		config:         config,
	}

	state.SetupCommands()
	return state
}

func getPrintMetricsCmd(state *embedEtcdMockState) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "print-metrics",
		Short: "print metrics restored from backup file",
		Run: func(cmd *cobra.Command, args []string) {

			node, err := cmd.Flags().GetString("node")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			metrics, ok := state.metrics[node]
			if !ok {
				fmt.Printf("not metrics found for node %s\n", node)
				return
			}
			fmt.Println(string(metrics))
		},
	}

	cmd.Flags().String("node", "", "select node metrics to print")
	return cmd
}

func readFixLengthHeader[T proto.Message](rd *bufio.Reader, header T) error {
	lb := make([]byte, 8)
	lenRead, err := rd.Read(lb)
	if err == io.EOF || lenRead < 8 {
		return fmt.Errorf("File does not contains valid header")
	}

	nextBytes := binary.LittleEndian.Uint64(lb)
	headerBs := make([]byte, nextBytes)
	lenRead, err = io.ReadFull(rd, headerBs)
	if err != nil {
		return fmt.Errorf("failed to read header bytes, %w", err)
	}
	if lenRead != int(nextBytes) {
		return fmt.Errorf("not enough bytes for header")
	}
	err = proto.Unmarshal(headerBs, header)
	if err != nil {
		return fmt.Errorf("failed to unmarshal header, err: %w", err)
	}
	return nil
}

// startEmbedEtcdServer start an embed etcd server to mock with backup data
func startEmbedEtcdServer(workspaceName string, useWorkspace bool) (*embed.Etcd, error) {
	var dir string
	var err error
	if useWorkspace {
		info, err := os.Stat(workspaceName)
		if err == nil {
			if info.IsDir() {
				dir = workspaceName
			}
		} else {
			fmt.Println(err.Error())
		}
	}
	if dir == "" {
		fmt.Println("[Start Embed Etcd]using temp dir")
		dir, err = ioutil.TempDir(os.TempDir(), "birdwatcher")
		if err != nil {
			return nil, err
		}
	}

	fmt.Println("embed etcd use dir:", dir)

	config := embed.NewConfig()

	config.Dir = dir
	config.LogLevel = "warn"
	config.LogOutputs = []string{"default"}
	u, err := url.Parse("http://localhost:0")
	if err != nil {
		return nil, err
	}
	config.LCUrls = []url.URL{*u}
	u, err = url.Parse("http://localhost:0")
	if err != nil {
		return nil, err
	}
	config.LPUrls = []url.URL{*u}

	return embed.StartEtcd(config)
}
