package common

import (
	"fmt"
	"io"
	"math"
	"sync"
)

const DefaultReadBufferSize = 10 << 20 // 10 MiB per open object.

// NewBufferedReadSeeker coalesces small reads of an immutable object into
// aligned range reads. Read and ReadAt share one lazily allocated buffer;
// requests at least bufferSize bytes bypass it. ReadAt does not change the
// logical seek position, even when the underlying object implementation does.
// Optional Close support and ownership are preserved. On construction errors
// the caller still owns source. An already buffered source is returned as-is.
func NewBufferedReadSeeker(source ReadSeeker, bufferSize int) (ReadSeeker, error) {
	if source == nil || bufferSize <= 0 {
		return nil, fmt.Errorf("buffered reader requires a source and a positive buffer size")
	}
	if _, ok := source.(interface{ readBufferSize() int }); ok {
		return source, nil
	}
	position, err := source.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	if position < 0 {
		return nil, fmt.Errorf("buffered reader source has negative position %d", position)
	}
	r := &bufferedReadSeeker{source: source, capacity: bufferSize, position: position}
	if closer, ok := source.(io.Closer); ok {
		return &bufferedReadCloser{bufferedReadSeeker: r, closer: closer}, nil
	}
	return r, nil
}

type bufferedReadSeeker struct {
	mu        sync.Mutex
	source    ReadSeeker
	capacity  int
	position  int64
	size      int64
	sizeKnown bool
	buffer    []byte
	start     int64
	valid     int
	bufferErr error
	closed    bool
}

func (r *bufferedReadSeeker) readBufferSize() int { return r.capacity }

func (r *bufferedReadSeeker) objectSize() (int64, error) {
	if !r.sizeKnown {
		size, err := r.source.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		if size < 0 {
			return 0, fmt.Errorf("buffered reader source has negative size %d", size)
		}
		r.size, r.sizeKnown = size, true
	}
	return r.size, nil
}

func (r *bufferedReadSeeker) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, err := r.readAt(p, r.position)
	r.position += int64(n)
	if n > 0 && err == io.EOF {
		err = nil
	}
	return n, err
}

func (r *bufferedReadSeeker) ReadAt(p []byte, offset int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readAt(p, offset)
}

func (r *bufferedReadSeeker) readAt(p []byte, offset int64) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if offset < 0 || int64(len(p)) > math.MaxInt64-offset {
		return 0, fmt.Errorf("invalid buffered read range: offset=%d length=%d", offset, len(p))
	}
	if len(p) == 0 {
		return 0, nil
	}
	size, err := r.objectSize()
	if err != nil {
		return 0, err
	}
	var total int
	for total < len(p) {
		if offset >= size {
			return total, io.EOF
		}
		if offset >= r.start && offset-r.start < int64(r.valid) {
			n := copy(p[total:], r.buffer[offset-r.start:r.valid])
			total += n
			offset += int64(n)
			if total == len(p) {
				return total, nil
			}
			if r.bufferErr != nil {
				return total, r.bufferErr
			}
			continue
		}
		// Do not split a large column-chunk read into many buffer-sized GETs.
		if len(p)-total >= r.capacity {
			want := min(int64(len(p)-total), size-offset)
			n, err := r.source.ReadAt(p[total:total+int(want)], offset)
			if n < int(want) && err == nil {
				err = io.ErrUnexpectedEOF
			}
			if n == int(want) && err == nil && total+n < len(p) {
				err = io.EOF
			}
			return total + n, err
		}
		// Align backward so footer probes and the following metadata read can
		// share a range, including when the object fits in a single buffer.
		r.start = offset / int64(r.capacity) * int64(r.capacity)
		want := int(min(int64(r.capacity), size-r.start))
		if len(r.buffer) < want {
			r.buffer = make([]byte, want)
		}
		r.valid, r.bufferErr = r.source.ReadAt(r.buffer[:want], r.start)
		if r.valid < want && r.bufferErr == nil {
			r.bufferErr = io.ErrUnexpectedEOF
		}
		if offset-r.start >= int64(r.valid) {
			return total, r.bufferErr
		}
	}
	return total, nil
}

func (r *bufferedReadSeeker) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = r.position
	case io.SeekEnd:
		var err error
		base, err = r.objectSize()
		if err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("invalid buffered seek whence %d", whence)
	}
	if offset < -base || (offset > 0 && base > math.MaxInt64-offset) {
		return 0, fmt.Errorf("invalid buffered seek offset %d", offset)
	}
	r.position = base + offset
	return r.position, nil
}

type bufferedReadCloser struct {
	*bufferedReadSeeker
	closer   io.Closer
	closeErr error
}

func (r *bufferedReadCloser) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.buffer = nil
		r.closeErr = r.closer.Close()
	}
	return r.closeErr
}
