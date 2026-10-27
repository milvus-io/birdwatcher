package common

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type scanDebugBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *scanDebugBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}
func (b *scanDebugBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buffer.String() }

type scanDebugTestSource struct {
	*bytes.Reader
	closed int
}

func (s *scanDebugTestSource) Close() error { s.closed++; return nil }

func TestScanDebugDisabledPreservesReader(t *testing.T) {
	ctx := context.Background()
	source := &scanDebugTestSource{Reader: bytes.NewReader([]byte("abc"))}
	require.Same(t, source, WithScanReadLogging(ctx, "file", source))
	require.Equal(t, ctx, WithScanScope(ctx, "worker=1"))
	require.False(t, ScanDebugEnabled(ctx))
	ScanDebug(ctx, "no logger")
	ScanOperation(ctx, "no logger")(io.EOF)
}

func TestScanDebugLogsIOAndKeepsCloserCapability(t *testing.T) {
	var out scanDebugBuffer
	ctx, stop := WithScanDebug(context.Background(), &out)
	defer stop()
	ctx = WithScanScope(ctx, "worker=%d segment=%d", 2, 99)
	source := &scanDebugTestSource{Reader: bytes.NewReader([]byte("abc"))}
	r := WithScanReadLogging(ctx, "s3://user:password@bucket/key?token=secret#fragment", source)
	buf := make([]byte, 2)
	n, err := r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, "ab", string(buf))
	pos, err := r.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Zero(t, pos)
	n, err = r.ReadAt(buf, 2)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, n)
	require.NoError(t, r.(io.Closer).Close())
	require.Equal(t, 1, source.closed)
	_, canClose := WithScanReadLogging(ctx, "memory", bytes.NewReader(nil)).(io.Closer)
	require.False(t, canClose)
	stop()
	log := out.String()
	for _, value := range []string{"worker=2 segment=99", "io.Read ", "io.ReadAt ", "offset=2 requested=2", "io.Seek ", "io.Close ", "bytes=1", "error=EOF", "s3://bucket/key", " start ", " done elapsed="} {
		require.Contains(t, log, value)
	}
	for _, secret := range []string{"password", "secret", "token=", "user:", "#fragment"} {
		require.NotContains(t, log, secret)
	}
}

type scanDebugBlockingSource struct {
	*bytes.Reader
	entered chan struct{}
	release chan struct{}
}

func (s *scanDebugBlockingSource) ReadAt(p []byte, offset int64) (int, error) {
	close(s.entered)
	<-s.release
	return s.Reader.ReadAt(p, offset)
}

func TestScanDebugReportsWaitingBeforeIOCompletes(t *testing.T) {
	var out scanDebugBuffer
	ctx, stop := withScanDebug(context.Background(), &out, 10*time.Millisecond)
	defer stop()
	source := &scanDebugBlockingSource{Reader: bytes.NewReader([]byte("a")), entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(source.release) })
	r := WithScanReadLogging(WithScanScope(ctx, "segment=77"), "blocked.parquet", source)
	done := make(chan error, 1)
	go func() { _, err := r.ReadAt(make([]byte, 1), 0); done <- err }()
	select {
	case <-source.entered:
	case <-time.After(time.Second):
		t.Fatal("read did not start")
	}
	require.Contains(t, out.String(), "start io.ReadAt")
	require.NotContains(t, out.String(), "done elapsed=")
	require.Eventually(t, func() bool { return strings.Contains(out.String(), "waiting elapsed=") }, time.Second, time.Millisecond)
	require.Contains(t, out.String(), `object="blocked.parquet" offset=0 requested=1`)
	release.Do(func() { close(source.release) })
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("read did not finish")
	}
	stop()
	require.Contains(t, out.String(), "segment=77 op=1 done elapsed=")
}

func TestScanDebugConcurrentScopesAndCleanup(t *testing.T) {
	var out scanDebugBuffer
	ctx, stop := withScanDebug(context.Background(), &out, 5*time.Millisecond)
	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := WithScanScope(ctx, "worker=%d", worker)
			for batch := range 10 {
				end := ScanOperation(ctx, "batch=%d", batch)
				ScanDebug(ctx, "processing")
				end(nil)
			}
		}()
	}
	wg.Wait()
	log := out.String()
	require.Equal(t, 40, strings.Count(log, " start "))
	require.Equal(t, 40, strings.Count(log, " done elapsed="))
	for worker := range 4 {
		require.Contains(t, log, fmt.Sprintf("worker=%d", worker))
	}
	end := ScanOperation(ctx, "pending when logger stops")
	stop()
	before := out.String()
	time.Sleep(15 * time.Millisecond)
	require.Equal(t, before, out.String())
	end(nil)
}
