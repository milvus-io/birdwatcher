package common

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

type scanDebugKey struct{}

type scanDebugContext struct {
	logger *scanDebugLogger
	scope  string
}

type scanDebugOperation struct {
	scope   string
	detail  string
	started time.Time
}

type scanDebugLogger struct {
	mu     sync.Mutex
	writer io.Writer
	nextID uint64
	active map[uint64]scanDebugOperation
}

// WithScanDebug enables command-local logging and reports pending operations
// every ten seconds. stop waits for the reporter to exit; call it on all paths.
func WithScanDebug(ctx context.Context, writer io.Writer) (context.Context, func()) {
	return withScanDebug(ctx, writer, 10*time.Second)
}

func withScanDebug(ctx context.Context, writer io.Writer, interval time.Duration) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	logger := &scanDebugLogger{writer: writer, active: make(map[uint64]scanDebugOperation)}
	ctx = context.WithValue(ctx, scanDebugKey{}, &scanDebugContext{logger: logger})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				logger.mu.Lock()
				var ids []uint64
				for id, op := range logger.active {
					if now.Sub(op.started) >= interval {
						ids = append(ids, id)
					}
				}
				sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
				for _, id := range ids {
					op := logger.active[id]
					logger.write(op.scope, "op=%d waiting elapsed=%s %s", id, now.Sub(op.started).Round(time.Millisecond), op.detail)
				}
				logger.mu.Unlock()
			}
		}
	}()
	return ctx, func() { cancel(); <-done }
}

// write is called with mu held, so concurrent workers emit complete log lines.
func (l *scanDebugLogger) write(scope, format string, args ...any) {
	fmt.Fprintf(l.writer, "%s [scan-binlog verbose] %s%s\n", time.Now().Format(time.RFC3339Nano), scope, fmt.Sprintf(format, args...))
}

func ScanDebugEnabled(ctx context.Context) bool {
	return ctx.Value(scanDebugKey{}) != nil
}

func WithScanScope(ctx context.Context, format string, args ...any) context.Context {
	if state, ok := ctx.Value(scanDebugKey{}).(*scanDebugContext); ok {
		return context.WithValue(ctx, scanDebugKey{}, &scanDebugContext{logger: state.logger, scope: state.scope + fmt.Sprintf(format, args...) + " "})
	}
	return ctx
}

func ScanDebug(ctx context.Context, format string, args ...any) {
	if state, ok := ctx.Value(scanDebugKey{}).(*scanDebugContext); ok {
		state.logger.mu.Lock()
		defer state.logger.mu.Unlock()
		state.logger.write(state.scope, format, args...)
	}
}

// ScanOperation logs before a potentially blocking step, then its result and
// elapsed time. The returned function is a no-op when verbose logging is off.
func ScanOperation(ctx context.Context, format string, args ...any) func(error, ...string) {
	state, ok := ctx.Value(scanDebugKey{}).(*scanDebugContext)
	if !ok {
		return quietScanOperation
	}
	l := state.logger
	l.mu.Lock()
	l.nextID++
	id := l.nextID
	op := scanDebugOperation{scope: state.scope, detail: fmt.Sprintf(format, args...), started: time.Now()}
	l.active[id] = op
	l.write(op.scope, "op=%d start %s", id, op.detail)
	l.mu.Unlock()
	return func(err error, result ...string) {
		l.mu.Lock()
		defer l.mu.Unlock()
		delete(l.active, id)
		l.write(op.scope, "op=%d done elapsed=%s %s result=%q error=%v", id, time.Since(op.started).Round(time.Microsecond), op.detail, strings.Join(result, " "), err)
	}
}

func quietScanOperation(error, ...string) {}

// NextScanRecord exposes the batch read that may block below a Parquet reader.
func NextScanRecord(ctx context.Context, reader interface {
	Next() bool
	Err() error
}, format string, args ...any,
) bool {
	if !ScanDebugEnabled(ctx) {
		return reader.Next()
	}
	end := ScanOperation(ctx, format, args...)
	next := reader.Next()
	end(reader.Err(), fmt.Sprintf("next=%t", next))
	return next
}

// ScanLogPath omits URI credentials and signed query strings from diagnostics.
func ScanLogPath(name string) string {
	if strings.Contains(name, "://") {
		if u, err := url.Parse(name); err == nil {
			u.User, u.RawQuery, u.Fragment = nil, "", ""
			return u.String()
		}
		return "<invalid URI>"
	}
	return name
}

// WithScanReadLogging keeps ReadSeeker and optional Closer capabilities while
// exposing the I/O performed by lazy object handles and Parquet readers.
func WithScanReadLogging(ctx context.Context, name string, source ReadSeeker) ReadSeeker {
	if !ScanDebugEnabled(ctx) {
		return source
	}
	r := &scanDebugReader{ReadSeeker: source, ctx: ctx, name: ScanLogPath(name)}
	if closer, ok := source.(io.Closer); ok {
		return &scanDebugCloser{scanDebugReader: r, closer: closer}
	}
	return r
}

type scanDebugReader struct {
	ReadSeeker
	ctx  context.Context
	name string
}

func (r *scanDebugReader) Read(p []byte) (n int, err error) {
	end := ScanOperation(r.ctx, "io.Read object=%q requested=%d", r.name, len(p))
	n, err = r.ReadSeeker.Read(p)
	end(err, fmt.Sprintf("bytes=%d", n))
	return n, err
}

func (r *scanDebugReader) ReadAt(p []byte, offset int64) (n int, err error) {
	end := ScanOperation(r.ctx, "io.ReadAt object=%q offset=%d requested=%d", r.name, offset, len(p))
	n, err = r.ReadSeeker.ReadAt(p, offset)
	end(err, fmt.Sprintf("bytes=%d", n))
	return n, err
}

func (r *scanDebugReader) Seek(offset int64, whence int) (position int64, err error) {
	end := ScanOperation(r.ctx, "io.Seek object=%q offset=%d whence=%d", r.name, offset, whence)
	position, err = r.ReadSeeker.Seek(offset, whence)
	end(err, fmt.Sprintf("position=%d", position))
	return position, err
}

type scanDebugCloser struct {
	*scanDebugReader
	closer io.Closer
}

func (r *scanDebugCloser) Close() error {
	end := ScanOperation(r.ctx, "io.Close object=%q", r.name)
	err := r.closer.Close()
	end(err)
	return err
}
