package states

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/oss"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
)

func TestScanBinlogReadBufferSizeFlag(t *testing.T) {
	for _, test := range []struct {
		args []string
		want int64
	}{{nil, 10 << 20}, {[]string{"--readBufferSize", "2097152"}, 2 << 20}, {[]string{"--readBufferSize", "0"}, 0}} {
		host := &struct{ *framework.CmdState }{framework.NewCmdState("test", &configs.Config{})}
		receiver := &scanBinlogFlagReceiver{}
		root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}
		host.MergeFunctionCommandsFrom(root, host, receiver)
		root.SetArgs(append([]string{"scan-binlog", "--collection", "1"}, test.args...))
		require.NoError(t, root.Execute())
		require.Equal(t, test.want, receiver.param.ReadBufferSize)
	}
	_, err := (&InstanceState{}).ScanBinlogCommand(context.Background(), &ScanBinlogParams{WorkerNum: 1, ReadBufferSize: -1})
	require.ErrorContains(t, err, "--readBufferSize")
}

type (
	scanBufferRead struct {
		offset int64
		size   int
	}
	scanBufferStore struct {
		oss.ObjectStore
		mu    sync.Mutex
		reads map[string][]scanBufferRead
	}
)

type scanBufferObject struct {
	storagecommon.ReadSeeker
	key   string
	store *scanBufferStore
}

func (s *scanBufferStore) Open(ctx context.Context, key string, opts ...oss.OpenOption) (storagecommon.ReadSeeker, error) {
	obj, err := s.ObjectStore.Open(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	return &scanBufferObject{obj, key, s}, nil
}

func (o *scanBufferObject) ReadAt(p []byte, offset int64) (int, error) {
	o.store.mu.Lock()
	o.store.reads[o.key] = append(o.store.reads[o.key], scanBufferRead{offset, len(p)})
	o.store.mu.Unlock()
	return o.ReadSeeker.ReadAt(p, offset)
}

func (o *scanBufferObject) Close() error {
	if closer, ok := o.ReadSeeker.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func TestScanBinlogBuffersV3ObjectsWithConfiguredSize(t *testing.T) {
	for _, size := range []int64{0, 32, 2 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			s, store, _ := newScanCommandFixture(t)
			counted := &scanBufferStore{ObjectStore: store, reads: make(map[string][]scanBufferRead)}
			s.objectStoreProvider = scanFixtureStoreProvider{counted}
			var output string
			debug, err := captureScanDebugOutput(t, func() error {
				var err error
				output, err = captureScanOutput(t, func() error {
					_, err := s.ScanBinlogCommand(context.Background(), &ScanBinlogParams{CollectionID: 1, Action: "count-distinct", Field: "value", WorkerNum: 1, ReadBufferSize: size, Verbose: true})
					return err
				})
				return err
			})
			require.NoError(t, err)
			require.Contains(t, output, "Total 2 distinct non-null values")
			actualSize := size
			if actualSize == 0 {
				actualSize = storagecommon.DefaultReadBufferSize
			}
			require.Contains(t, debug, fmt.Sprintf("v3.read_buffer bytes=%d", actualSize))
			for _, key := range []string{"files/insert_log/1/2/20/_data/pk", "files/insert_log/1/2/20/_data/value", "files/insert_log/1/2/20/_delta/delete"} {
				require.NotEmpty(t, counted.reads[key])
				if actualSize >= int64(len(store.data[key])) {
					require.Equal(t, []scanBufferRead{{0, len(store.data[key])}}, counted.reads[key])
				} else {
					require.Greater(t, len(counted.reads[key]), 1)
					require.LessOrEqual(t, counted.reads[key][0].size, int(actualSize))
				}
			}
			require.Equal(t, store.opens, store.closes)
		})
	}
}
