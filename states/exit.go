package states

import (
	"context"

	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/spf13/cobra"
)

// getExitCmd returns exit command for input state.
func getExitCmd(state common.State) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "exit",
		Short:   "Closes the cli",
		Aliases: []string{"quit"},
		RunE: func(*cobra.Command, []string) error {
			state.SetNext(&common.ExitState{})
			// cannot return ExitErr here to avoid print help message
			return nil
		},
	}
	return cmd
}

type DisconnectParam struct {
	framework.ParamBase `use:"disconnect" desc:"disconnect from current etcd instance"`
}

func (s *InstanceState) DisconnectCommand(ctx context.Context, _ *DisconnectParam) {
	s.SetNext(Start(s.config))
	s.Close()
}
