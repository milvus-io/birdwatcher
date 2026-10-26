package states

import (
	"context"
	"testing"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type scanBinlogFlagReceiver struct{ param *ScanBinlogParams }

func (r *scanBinlogFlagReceiver) ScanBinlogCommand(_ context.Context, p *ScanBinlogParams) error {
	r.param = p
	return nil
}

func TestScanBinlogOverrideFlags(t *testing.T) {
	host := &struct{ *framework.CmdState }{framework.NewCmdState("test", &configs.Config{})}
	receiver := &scanBinlogFlagReceiver{}
	root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}
	host.MergeFunctionCommandsFrom(root, host, receiver)
	root.SetArgs([]string{"scan-binlog", "--collection", "123", "--ossStandalone", "--useIAM", "true", "--useSSL", "false", "--cloudProvider", "aws", "--bucketName", "bucket", "--rootPath", "files", "--minioAddr", "localhost:9000"})
	require.NoError(t, root.Execute())
	require.NotNil(t, receiver.param)
	require.Equal(t, int64(123), receiver.param.CollectionID)
	require.Equal(t, "true", receiver.param.UseIAM)
	require.Equal(t, "false", receiver.param.UseSSL)
	require.Equal(t, "aws", receiver.param.CloudProvider)
	require.Equal(t, "bucket", receiver.param.BucketName)
	require.Equal(t, "files", receiver.param.RootPath)
	require.Equal(t, "localhost:9000", receiver.param.MinioAddress)
	require.True(t, receiver.param.OSSStandalone)
}
