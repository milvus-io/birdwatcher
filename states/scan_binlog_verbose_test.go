package states

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
)

func TestScanBinlogVerboseFlag(t *testing.T) {
	for _, value := range []string{"", "--verbose", "--verbose=false"} {
		t.Run(value, func(t *testing.T) {
			host := &struct{ *framework.CmdState }{framework.NewCmdState("test", &configs.Config{})}
			receiver := &scanBinlogFlagReceiver{}
			root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}
			host.MergeFunctionCommandsFrom(root, host, receiver)
			args := []string{"scan-binlog", "--collection", "123"}
			if value != "" {
				args = append(args, value)
			}
			root.SetArgs(args)
			require.NoError(t, root.Execute())
			require.Equal(t, value == "--verbose", receiver.param.Verbose)
		})
	}
}

func captureScanDebugOutput(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	old := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = old; r.Close(); w.Close() }()
	done := make(chan string, 1)
	go func() { data, _ := io.ReadAll(r); done <- string(data) }()
	err = fn()
	w.Close()
	return <-done, err
}

func TestScanBinlogVerboseStagesAndDefaultSilence(t *testing.T) {
	for _, verbose := range []bool{false, true} {
		t.Run(fmt.Sprint(verbose), func(t *testing.T) {
			s, store, _ := newScanCommandFixture(t)
			var output string
			debug, err := captureScanDebugOutput(t, func() error {
				var err error
				output, err = captureScanOutput(t, func() error {
					_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, SegmentID: 20, Action: "count", Fields: []string{"value"}, WorkerNum: 2, Verbose: verbose})
					return err
				})
				return err
			})
			require.NoError(t, err)
			require.Contains(t, output, "Total 2 entries found")
			require.Equal(t, store.opens, store.closes)
			require.NotContains(t, output, "[scan-binlog verbose]")
			if !verbose {
				require.Empty(t, debug)
				return
			}
			for _, stage := range []string{"[scan-binlog verbose]", "metadata.collection", "metadata.segments", "object_store.internal.init", "fields.selected ids=[1 100 101]", "queue.dispatch segment=20", "worker=", "segment=20", "manifest.parse", "manifest.loaded version=7 groups=2", "deletes.load", "deltalog.format packed_parquet", "deltalog.arrow.next", "reader.manifest.init", "column_group=", "parquet.metadata", "parquet.project", "parquet.arrow.next", "manifest.batch.ready", "rows.processed", "rows_read=4 task_counter=2", "io.ReadAt", "io.Seek", "io.Close", "done elapsed="} {
				require.Contains(t, debug, stage)
			}
		})
	}
}

func TestScanBinlogVerboseReportsFailureAndSkipsDeletes(t *testing.T) {
	s, store, _ := newScanCommandFixture(t)
	delete(store.data, "files/insert_log/1/2/20/_delta/delete")
	debug, err := captureScanDebugOutput(t, func() error {
		_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, Action: "count", WorkerNum: 1, Verbose: true})
		return err
	})
	require.ErrorContains(t, err, "open deltalog")
	require.Contains(t, debug, "error=missing fixture object")
	require.Contains(t, debug, "command.scan")
	debug, err = captureScanDebugOutput(t, func() error {
		_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, Action: "count", WorkerNum: 1, Verbose: true, IgnoreDelete: true})
		return err
	})
	require.NoError(t, err)
	require.Contains(t, debug, "deletes.skipped reason=ignoreDelete")
	require.NotContains(t, debug, "deltalog.decode")
}
