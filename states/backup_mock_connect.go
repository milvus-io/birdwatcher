package states

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"

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
	if s.server != nil {
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
		// clean-empty-segment-by-id
		cleanEmptySegmentByID(s.client, path.Join(s.instanceName, metaPath)),
		// disconnect
		getDisconnectCmd(s),

		// raw get
		getEtcdRawCmd(s.client),

		// exit
		getExitCmd(s),
	)
	cmd.AddCommand(getGlobalUtilCommands()...)

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
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

			server, err := startEmbedEtcdServer()
			if err != nil {
				fmt.Println("failed to start embed etcd server:", err.Error())
				return
			}
			fmt.Println("using data dir:", server.Config().Dir)

			var rootPath string
			client := v3client.New(server.Server)
			rootPath, _, err = restoreEtcd(client, arg)
			if err != nil {
				fmt.Printf("failed to restore file: %s, error: %s", arg, err.Error())
				server.Close()
				return
			}

			state.SetNext(getEmbedEtcdInstance(server, client, rootPath))
		},
	}

	return cmd
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
