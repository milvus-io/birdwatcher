package framework

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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

type testTypoParam struct {
	ParamBase `use:"typo" desc:"typo command"`
}

type testTypoReceiver struct{}

func (r *testTypoReceiver) TypoCommnad(ctx context.Context, p *testTypoParam) error { return nil }

func (r *testTypoReceiver) RealCommand(ctx context.Context, p *testTypoParam) error { return nil }

func (r *testTypoReceiver) NotACommand() {}

func TestFindMissingCommandSuffixDetectsTypo(t *testing.T) {
	got := FindMissingCommandSuffix(&testTypoReceiver{})
	require.Equal(t, []string{"TypoCommnad"}, got)
}

func TestFindMissingCommandSuffixCleanReceiver(t *testing.T) {
	got := FindMissingCommandSuffix(&testExternalReceiver{})
	require.Empty(t, got)
}

func TestFindMissingCommandSuffixNilReceiver(t *testing.T) {
	require.Empty(t, FindMissingCommandSuffix(nil))
	require.Empty(t, FindMissingCommandSuffix((*testTypoReceiver)(nil)))
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

func TestRedactCommand(t *testing.T) {
	registerSecretFlag("accessKeyID")
	registerSecretFlag("secretAccessKey")
	registerSecretFlag("api-key")
	registerSecretFlag("password")
	for _, tc := range []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "space separated secret flags",
			input:    "scan-binlog --accessKeyID AK123 --secretAccessKey sk456 --collection 123",
			expected: "scan-binlog --accessKeyID [REDACTED] --secretAccessKey [REDACTED] --collection 123",
		},
		{
			name:     "inline secret flag values",
			input:    "connect milvus --api-key=abc123 --address localhost:19530",
			expected: "connect milvus --api-key=[REDACTED] --address localhost:19530",
		},
		{
			name:     "non-secret flags untouched",
			input:    "show collection --collection 10 --verbose",
			expected: "show collection --collection 10 --verbose",
		},
		{
			name:     "mixed inline and space forms",
			input:    "connect milvus --api-key=abc --password pw --tls=true",
			expected: "connect milvus --api-key=[REDACTED] --password [REDACTED] --tls=true",
		},
		{
			name:     "secret flag without value at end",
			input:    "scan-binlog --accessKeyID",
			expected: "scan-binlog --accessKeyID",
		},
		{
			name:     "empty input",
			input:    "",
			expected: "",
		},
		{
			name:     "command without secret flags",
			input:    "debug commands",
			expected: "debug commands",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, RedactCommand(tc.input))
		})
	}
}

type testSecretParam struct {
	ParamBase `use:"secret-cmd" desc:"command with secret flags"`
	AccessKey string `name:"accessKeyID" default:"" secret:"true"`
	Bucket    string `name:"bucketName" default:""`
}

func TestSetupFlagsRegistersSecretTags(t *testing.T) {
	cp := &testSecretParam{}
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	setupFlags(cp, flags)

	require.True(t, isSecretFlag("accessKeyID"))
	require.False(t, isSecretFlag("bucketName"))

	input := "secret-cmd --accessKeyID AK123 --bucketName bucket"
	require.Equal(t, "secret-cmd --accessKeyID [REDACTED] --bucketName bucket", RedactCommand(input))
}
