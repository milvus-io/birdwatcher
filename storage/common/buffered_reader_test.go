package common

import (
	"bytes"
	"errors"
	"io"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type bufferTestRead struct {
	offset int64
	size   int
}

type bufferTestSource struct {
	*bytes.Reader
	mu       sync.Mutex
	reads    []bufferTestRead
	endSeeks int
	closed   int
}

func (s *bufferTestSource) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads = append(s.reads, bufferTestRead{off, len(p)})
	n, err := s.Reader.ReadAt(p, off)
	// MinIO's ReadAt changes the object's current offset. The buffered
	// reader must keep its own cursor so this does not corrupt sequential reads.
	s.Reader.Seek(off+int64(n), io.SeekStart)
	return n, err
}

func (s *bufferTestSource) Seek(off int64, whence int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if whence == io.SeekEnd {
		s.endSeeks++
	}
	return s.Reader.Seek(off, whence)
}

func (s *bufferTestSource) Close() error { s.mu.Lock(); defer s.mu.Unlock(); s.closed++; return nil }

func TestBufferedReaderCoalescesSmallRanges(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 4)
	source := &bufferTestSource{Reader: bytes.NewReader(data)}
	r, err := NewBufferedReadSeeker(source, 16)
	require.NoError(t, err)
	for off := range len(data) {
		buf := make([]byte, 1)
		n, err := r.ReadAt(buf, int64(off))
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.Equal(t, data[off:off+1], buf)
	}
	require.Equal(t, []bufferTestRead{{0, 16}, {16, 16}, {32, 16}, {48, 16}}, source.reads)
	require.Equal(t, 1, source.endSeeks)
	require.LessOrEqual(t, len(r.(*bufferedReadCloser).buffer), 16)
	require.NoError(t, r.(io.Closer).Close())
	require.NoError(t, r.(io.Closer).Close())
	require.Equal(t, 1, source.closed)
}

func TestBufferedReaderFooterProbeSharesCacheAndRetainsCursor(t *testing.T) {
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	source := &bufferTestSource{Reader: bytes.NewReader(data)}
	_, err := source.Seek(3, io.SeekStart)
	require.NoError(t, err)
	r, err := NewBufferedReadSeeker(source, DefaultReadBufferSize)
	require.NoError(t, err)
	footer := make([]byte, 8)
	_, err = r.ReadAt(footer, int64(len(data)-8))
	require.NoError(t, err)
	require.Equal(t, data[len(data)-8:], footer)
	pos, err := r.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	require.EqualValues(t, 3, pos)
	buf := make([]byte, 5)
	_, err = io.ReadFull(r, buf)
	require.NoError(t, err)
	require.Equal(t, data[3:8], buf)
	pos, err = r.Seek(-2, io.SeekCurrent)
	require.NoError(t, err)
	require.EqualValues(t, 6, pos)
	_, err = r.ReadAt(buf, 1)
	require.NoError(t, err)
	require.Equal(t, data[1:6], buf)
	pos, err = r.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	require.EqualValues(t, 6, pos)
	require.Equal(t, []bufferTestRead{{0, len(data)}}, source.reads)
	pos, err = r.Seek(-2, io.SeekEnd)
	require.NoError(t, err)
	require.EqualValues(t, len(data)-2, pos)
	n, err := r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, data[len(data)-2:], buf[:n])
	n, err = r.Read(buf)
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, n)
	require.Equal(t, 1, source.endSeeks)
}

func TestBufferedReaderCrossesBlocksAndEOF(t *testing.T) {
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	source := &bufferTestSource{Reader: bytes.NewReader(data)}
	r, err := NewBufferedReadSeeker(source, 16)
	require.NoError(t, err)
	buf := make([]byte, 6)
	n, err := r.ReadAt(buf, 14)
	require.NoError(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, data[14:20], buf)
	require.Equal(t, []bufferTestRead{{0, 16}, {16, 16}}, source.reads)
	buf = make([]byte, 12)
	n, err = r.ReadAt(buf, 30)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 6, n)
	require.Equal(t, data[30:], buf[:n])
	require.Equal(t, bufferTestRead{32, 4}, source.reads[len(source.reads)-1])
	n, err = r.ReadAt(buf, 100)
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, n)
	n, err = r.ReadAt(nil, 100)
	require.NoError(t, err)
	require.Zero(t, n)
	_, err = r.ReadAt(buf, -1)
	require.Error(t, err)
	_, err = r.ReadAt(buf, math.MaxInt64)
	require.Error(t, err)
	_, err = r.Seek(-1, io.SeekStart)
	require.Error(t, err)
	_, err = r.Seek(0, 123)
	require.Error(t, err)
	_, err = r.Seek(math.MaxInt64, io.SeekStart)
	require.NoError(t, err)
	_, err = r.Seek(1, io.SeekCurrent)
	require.Error(t, err)
}

func TestBufferedReaderBypassesLargeReadsAndAvoidsDoubleBuffering(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 8)
	source := &bufferTestSource{Reader: bytes.NewReader(data)}
	r, err := NewBufferedReadSeeker(source, 16)
	require.NoError(t, err)
	double, err := NewBufferedReadSeeker(r, 32)
	require.NoError(t, err)
	require.Same(t, r, double)
	buf := make([]byte, 40)
	n, err := r.ReadAt(buf, 3)
	require.NoError(t, err)
	require.Equal(t, 40, n)
	require.Equal(t, data[3:43], buf)
	require.Equal(t, []bufferTestRead{{3, 40}}, source.reads)
	require.Nil(t, r.(*bufferedReadCloser).buffer)
	buf = make([]byte, 40)
	n, err = r.ReadAt(buf, 100)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 28, n)
	require.Equal(t, data[100:], buf[:n])
	require.Equal(t, bufferTestRead{100, 28}, source.reads[1])
	require.NoError(t, r.(io.Closer).Close())
	_, err = r.ReadAt(buf, 0)
	require.ErrorIs(t, err, io.ErrClosedPipe)
	_, err = r.Read(buf)
	require.ErrorIs(t, err, io.ErrClosedPipe)
	_, err = r.Seek(0, io.SeekStart)
	require.ErrorIs(t, err, io.ErrClosedPipe)
	_, hasClose := func() ReadSeeker {
		v, err := NewBufferedReadSeeker(bytes.NewReader(data), 16)
		require.NoError(t, err)
		return v
	}().(io.Closer)
	require.False(t, hasClose)
}

type bufferFaultSource struct {
	*bytes.Reader
	fault error
}

func (s *bufferFaultSource) ReadAt(p []byte, off int64) (int, error) {
	n, _ := s.Reader.ReadAt(p[:min(len(p), 2)], off)
	return n, s.fault
}

func TestBufferedReaderPropagatesShortReadErrors(t *testing.T) {
	broken := errors.New("injected range read failure")
	for _, fault := range []error{broken, nil} {
		r, err := NewBufferedReadSeeker(&bufferFaultSource{bytes.NewReader([]byte("0123456789abcdef")), fault}, 16)
		require.NoError(t, err)
		buf := make([]byte, 4)
		n, err := r.ReadAt(buf, 0)
		require.Equal(t, 2, n)
		require.Equal(t, "01", string(buf[:n]))
		if fault != nil {
			require.ErrorIs(t, err, broken)
		} else {
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		}
	}
	for _, size := range []int{0, -1} {
		_, err := NewBufferedReadSeeker(bytes.NewReader(nil), size)
		require.Error(t, err)
	}
	_, err := NewBufferedReadSeeker(nil, 16)
	require.Error(t, err)
}

func TestBufferedReaderConcurrentReadAt(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 256)
	source := &bufferTestSource{Reader: bytes.NewReader(data)}
	r, err := NewBufferedReadSeeker(source, DefaultReadBufferSize)
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for worker := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				off := (worker*100 + i) % (len(data) - 8)
				buf := make([]byte, 8)
				if _, err := r.ReadAt(buf, int64(off)); err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(buf, data[off:off+8]) {
					errs <- errors.New("incorrect cached range")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, source.reads, 1)
	pos, err := r.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	require.Zero(t, pos)
}
