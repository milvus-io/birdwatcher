package states

import (
	"context"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
)

type testExtensionMarker struct{}

func TestWithExtensionsStoresExtensions(t *testing.T) {
	ext := &testExtensionMarker{}

	app, ok := Start(&configs.Config{}, false, WithExtensions(ext)).(*ApplicationState)
	require.True(t, ok)
	require.Len(t, app.extensions, 1)
	require.Same(t, ext, app.extensions[0])
}

type testInstanceExtension struct {
	cobraCtx InstanceContext
	fnCtx    InstanceContext
	receiver *testInstanceReceiver
}

func (e *testInstanceExtension) InstanceCobraCommands(ctx InstanceContext) []*cobra.Command {
	e.cobraCtx = ctx
	return []*cobra.Command{{Use: "extension-cobra", Short: "extension cobra command"}}
}

func (e *testInstanceExtension) InstanceFunctionCommands(ctx InstanceContext) []any {
	e.fnCtx = ctx
	e.receiver = &testInstanceReceiver{}
	return []any{e.receiver}
}

type testInstanceFunctionParam struct {
	framework.ParamBase `use:"extension function" desc:"extension function command"`
	Value               string `name:"value" default:"" desc:"value"`
}

type testInstanceReceiver struct {
	called bool
	value  string
}

func (r *testInstanceReceiver) FunctionCommand(ctx context.Context, p *testInstanceFunctionParam) error {
	r.called = true
	r.value = p.Value
	return nil
}

func TestInstanceStateMergesExtensionCommands(t *testing.T) {
	config := &configs.Config{}
	client := metakv.NewFileAuditKV(nil, nil)
	ext := &testInstanceExtension{}
	state := &InstanceState{
		CmdState:     framework.NewCmdState("Milvus(test)", config),
		instanceName: "test-root",
		metaPath:     "custom-meta",
		client:       client,
		config:       config,
		basePath:     "test-root/custom-meta",
		extensions:   []Extension{ext},
	}

	state.SetupCommands()

	_, _, err := state.RootCmd.Find([]string{"extension-cobra"})
	require.NoError(t, err)
	_, _, err = state.RootCmd.Find([]string{"extension", "function"})
	require.NoError(t, err)
	_, _, err = state.RootCmd.Find([]string{"dry-mode"})
	require.NoError(t, err)
	_, _, err = state.RootCmd.Find([]string{"show"})
	require.NoError(t, err)

	require.Same(t, state, ext.cobraCtx)
	require.Same(t, state, ext.fnCtx)
	require.Equal(t, "test-root", ext.cobraCtx.InstanceName())
	require.Equal(t, "custom-meta", ext.cobraCtx.MetaPath())
	require.Equal(t, "test-root/custom-meta", ext.cobraCtx.BasePath())
	require.Same(t, client, ext.cobraCtx.Client())
	require.Same(t, config, ext.cobraCtx.Config())
	require.Same(t, state, ext.cobraCtx.State())

	state.RootCmd.SetArgs([]string{"extension", "function", "--value", "ok"})
	err = state.RootCmd.Execute()
	require.NoError(t, err)
	require.True(t, ext.receiver.called)
	require.Equal(t, "ok", ext.receiver.value)
}
