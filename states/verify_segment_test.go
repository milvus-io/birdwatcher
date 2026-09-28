package states

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/framework"
)

func TestVerifySegmentCommandRegistered(t *testing.T) {
	s := &InstanceState{CmdState: framework.NewCmdState("test", nil)}
	cmd := &cobra.Command{}
	s.MergeFunctionCommandsFrom(cmd, s, s)

	found, _, err := cmd.Find([]string{"verify-segment"})
	require.NoError(t, err)
	require.NotNil(t, found)
	require.Equal(t, "verify-segment", found.Use)
}

func TestInstanceStateNoUnregisteredCommandLikeMethods(t *testing.T) {
	s := &InstanceState{CmdState: framework.NewCmdState("test", nil)}

	missing := framework.FindMissingCommandSuffix(s)
	require.Empty(t, missing,
		"these methods look like CLI commands but are missing the \"Command\" suffix, "+
			"so they are silently skipped during registration: %v", missing)
}
