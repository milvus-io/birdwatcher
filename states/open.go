package states

import (
	"fmt"
	"os"
	"path"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/spf13/cobra"
)

// getOpenWorkspaceCmd returns open-workspace command.
func getOpenWorkspaceCmd(state State, config *configs.Config) *cobra.Command {
	cmd := &cobra.Command{
		//TODO add workspace auto complete
		Use:   "open-workspace [workspace name]",
		Short: "Open workspace",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				fmt.Println("No backup file provided.")
				return
			}
			if len(args) > 1 {
				fmt.Println("only one backup file is allowed")
				return
			}

			workspaceName := args[0]
			workPath := path.Join(config.WorkspacePath, workspaceName)
			info, err := os.Stat(workPath)
			if os.IsNotExist(err) {
				fmt.Printf("workspace %s not exist\n", workspaceName)
				return
			}
			if !info.IsDir() {
				fmt.Printf("workspace %s is not a directory\n", workspaceName)
				return
			}

			server, err := startEmbedEtcdServer(workPath, true)
			if err != nil {
				fmt.Printf("failed to start embed etcd server in workspace %s, err: %s\n", workspaceName, err.Error())
				return
			}

			nextState := getEmbedEtcdInstanceV2(server, config)
			err = nextState.setupWorkDir(workPath)
			if err != nil {
				fmt.Printf("failed to setup workspace for %s, err: %s\n", workspaceName, err.Error())
			}

			state.SetNext(nextState)
		},
	}

	return cmd
}
