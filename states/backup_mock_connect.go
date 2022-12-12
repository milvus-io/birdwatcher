package states

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

type embedEtcdMockState struct {
	cmdState
	client       *clientv3.Client
	server       *embed.Etcd
	instanceName string
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

	cmd.AddCommand(
		// show [subcommand] options...
		getEtcdShowCmd(s.client, path.Join(s.instanceName, metaPath)),
		// download-pk
		getDownloadPKCmd(s.client, path.Join(s.instanceName, metaPath)),
		// inspect-pk
		getInspectPKCmd(s.client, path.Join(s.instanceName, metaPath)),
		// clean-empty-segment
		cleanEmptySegments(s.client, path.Join(s.instanceName, metaPath)),
		// remove-segment-by-id
		removeSegmentByID(s.client, path.Join(s.instanceName, metaPath)),
		// force-release
		getForceReleaseCmd(s.client, path.Join(s.instanceName, metaPath)),

		// disconnect
		getDisconnectCmd(s),

		// repair-segment
		getRepairSegmentCmd(s.client, path.Join(s.instanceName, metaPath)),
		// repair-channel
		getRepairChannelCmd(s.client, path.Join(s.instanceName, metaPath)),

		// raw get
		getEtcdRawCmd(s.client),

		// exit
		getExitCmd(s),
	)
	cmd.AddCommand(getGlobalUtilCommands()...)

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
}

func (s *embedEtcdMockState) SetInstance(instanceName string) {
	s.cmdState.label = fmt.Sprintf("Backup(%s)", instanceName)
	s.instanceName = instanceName
	s.SetupCommands()
}

func getEmbedEtcdInstance(server *embed.Etcd, cli *clientv3.Client, instanceName string) State {

	state := &embedEtcdMockState{
		cmdState: cmdState{
			label: fmt.Sprintf("Backup(%s)", instanceName),
		},
		instanceName: instanceName,
		server:       server,
		client:       cli,
	}

	state.SetupCommands()

	return state
}

func getEmbedEtcdInstanceV2(server *embed.Etcd) *embedEtcdMockState {

	client := v3client.New(server.Server)
	state := &embedEtcdMockState{
		cmdState: cmdState{},
		server:   server,
		client:   client,
	}

	state.SetupCommands()
	return state
}

func getLoadBackupCmd(state State) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "load-backup [file]",
		Short: "load etcd backup file as env",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				fmt.Println("No backup file provided.")
				return
			}
			if len(args) > 1 {
				fmt.Println("only one backup file is allowed")
				return
			}

			arg := args[0]
			if strings.Contains(arg, "~") {
				var err error
				arg, err = homedir.Expand(arg)
				if err != nil {
					fmt.Println("path contains tilde, but cannot find home folder", err.Error())
					return
				}
			}
			err := testFile(arg)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			f, err := os.Open(arg)
			if err != nil {
				fmt.Printf("failed to open backup file %s, err: %s\n", arg, err.Error())
				return
			}
			r, err := gzip.NewReader(f)
			if err != nil {
				fmt.Println("failed to open gzip reader, err:", err.Error())
				return
			}
			defer r.Close()

			rd := bufio.NewReader(r)
			var header models.BackupHeader
			err = readFixLengthHeader(rd, &header)
			if err != nil {
				fmt.Println("failed to load backup header", err.Error())
				return
			}

			server, err := startEmbedEtcdServer()
			if err != nil {
				fmt.Println("failed to start embed etcd server:", err.Error())
				return
			}
			fmt.Println("using data dir:", server.Config().Dir)
			nextState := getEmbedEtcdInstanceV2(server)
			switch header.Version {
			case 1:
				err = restoreFromV1File(nextState.client, rd, &header)
				if err != nil {
					fmt.Println("failed to restore v1 backup file", err.Error())
					nextState.Close()
					return
				}
				nextState.SetInstance(header.Instance)
			case 2:
				err = restoreV2File(rd, nextState)
				if err != nil {
					fmt.Println("failed to restore v2 backup file", err.Error())
					nextState.Close()
					return
				}
			default:
				fmt.Printf("backup version %d not supported\n", header.Version)
				nextState.Close()
				return
			}

			state.SetNext(nextState)
		},
	}

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

// testFile check file path exists and has access
func testFile(file string) error {
	fi, err := os.Stat(file)
	if err != nil {
		return err
	}
	// not support iterate all possible file under directory for now
	if fi.IsDir() {
		return fmt.Errorf("%s is a folder", file)
	}
	return nil
}

// startEmbedEtcdServer start an embed etcd server to mock with backup data
func startEmbedEtcdServer() (*embed.Etcd, error) {
	dir, err := ioutil.TempDir(os.TempDir(), "birdwatcher")
	if err != nil {
		return nil, err
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
