package framework

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/configs"
)

type testExternalParam struct {
	ParamBase `use:"external foo" desc:"external command"`
	Name      string `name:"name" default:"" desc:"name value"`
}

type testExternalReceiver struct {
	called bool
	name   string
}

func (r *testExternalReceiver) FooCommand(ctx context.Context, p *testExternalParam) error {
	r.called = true
	r.name = p.Name
	return nil
}

func TestMergeFunctionCommandsFromRegistersExternalReceiver(t *testing.T) {
	host := &struct{ *CmdState }{CmdState: NewCmdState("host", &configs.Config{})}
	receiver := &testExternalReceiver{}
	root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}

	items := parseFunctionCommandsFrom(host, receiver)
	require.Len(t, items, 1)
	host.MergeFunctionCommandsFrom(root, host, receiver)
	require.NotEmpty(t, root.Commands())

	root.SetArgs([]string{"external", "foo", "--name", "bird"})
	err := root.Execute()
	require.NoError(t, err)
	require.True(t, receiver.called)
	require.Equal(t, "bird", receiver.name)
}

type testLocalParam struct {
	ParamBase `use:"local" desc:"local command"`
}

type testLocalState struct {
	*CmdState
	called bool
}

func (s *testLocalState) LocalCommand(ctx context.Context, p *testLocalParam) error {
	s.called = true
	return nil
}

func TestMergeFunctionCommandsKeepsStateReceiverBehavior(t *testing.T) {
	state := &testLocalState{CmdState: NewCmdState("state", &configs.Config{})}
	root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}

	state.MergeFunctionCommands(root, state)

	root.SetArgs([]string{"local"})
	err := root.Execute()
	require.NoError(t, err)
	require.True(t, state.called)
}

type testFormatParam struct {
	ParamBase `use:"format" desc:"format command"`
}

type testFormatReceiver struct{}

func (r *testFormatReceiver) GetGlobalFormat() Format { return FormatJSON }

func (r *testFormatReceiver) FormatCommand(ctx context.Context, p *testFormatParam) (ResultSet, error) {
	return testFormatResultSet{}, nil
}

type testFormatResultSet struct{}

func (rs testFormatResultSet) PrintAs(format Format) string {
	return fmt.Sprintf("format:%d", format)
}

func (rs testFormatResultSet) Entities() any { return nil }

func TestMergeFunctionCommandsFromPrefersReceiverFormat(t *testing.T) {
	host := &struct{ *CmdState }{CmdState: NewCmdState("host", &configs.Config{OutputFormat: "plain"})}
	receiver := &testFormatReceiver{}
	root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}
	host.MergeFunctionCommandsFrom(root, host, receiver)

	output := captureStdout(t, func() {
		root.SetArgs([]string{"format"})
		err := root.Execute()
		require.NoError(t, err)
	})

	require.Equal(t, fmt.Sprintf("format:%d", FormatJSON), strings.TrimSpace(output))
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	fn()

	require.NoError(t, w.Close())
	os.Stdout = old
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(out)
}
