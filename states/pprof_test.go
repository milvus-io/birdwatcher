package states

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// pprofTestKV is a minimal kv.MetaKV fake that serves a fixed list of
// sessions for common.ListSessions to consume.
type pprofTestKV struct {
	kv.MetaKV

	sessions []*models.Session
}

func (k *pprofTestKV) LoadWithPrefix(ctx context.Context, prefix string, opts ...kv.LoadOption) ([]string, []string, error) {
	keys := make([]string, 0, len(k.sessions))
	vals := make([]string, 0, len(k.sessions))
	for _, session := range k.sessions {
		bs, err := json.Marshal(session)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, fmt.Sprintf("%s/%d", prefix, session.ServerID))
		vals = append(vals, string(bs))
	}
	return keys, vals, nil
}

// readTarEntries extracts every entry name and payload from a gzip'd tar file.
func readTarEntries(t *testing.T, path string) map[string][]byte {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	gr, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer gr.Close()

	tr := tar.NewReader(gr)
	entries := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		bs, err := io.ReadAll(tr)
		require.NoError(t, err)
		entries[hdr.Name] = bs
	}
	return entries
}

// captureStdout runs fn while redirecting os.Stdout and returns what was written.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	require.NoError(t, err)
	orig := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	outCh := make(chan string, 1)
	go func() {
		bs, _ := io.ReadAll(r)
		outCh <- string(bs)
	}()

	fn()
	require.NoError(t, w.Close())
	return <-outCh
}

func TestGetPprofCommandSkipsUnreachableNode(t *testing.T) {
	// a fake pprof endpoint that returns real data for the "good" node.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("REAL-GOROUTINE-DUMP-DATA"))
	}))
	defer srv.Close()

	_, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.ParseInt(portStr, 10, 64)
	require.NoError(t, err)

	// the pprof port is shared across every node (it comes from PprofParam,
	// not the session's own address), so simulate an unreachable node with a
	// distinct loopback IP that nothing listens on rather than a dead port.
	goodSession := &models.Session{ServerID: 1, ServerName: "nodeA", Address: fmt.Sprintf("127.0.0.1:%d", port)}
	badSession := &models.Session{ServerID: 2, ServerName: "nodeB", Address: "127.0.0.2:1"}

	s := &InstanceState{
		client:   &pprofTestKV{sessions: []*models.Session{goodSession, badSession}},
		basePath: "by-dev/meta",
	}

	dir := t.TempDir()
	t.Chdir(dir)

	var cmdErr error
	out := captureStdout(t, func() {
		cmdErr = s.GetPprofCommand(context.Background(), &PprofParam{Type: "goroutine", Port: port})
	})
	require.NoError(t, cmdErr)

	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 1, "expected exactly one archive file to be created")

	entries := readTarEntries(t, files[0].Name())

	// the unreachable node must not produce a fabricated archive entry.
	_, hasFailedEntry := entries["nodeB_2_goroutine"]
	require.False(t, hasFailedEntry, "unreachable node must not appear in the archive")

	// the reachable node's real data must be archived intact.
	require.Equal(t, []byte("REAL-GOROUTINE-DUMP-DATA"), entries["nodeA_1_goroutine"])
	require.Len(t, entries, 1)

	require.Contains(t, out, "failed to fetch goroutine pprof from nodeB-2")
	require.NotContains(t, out, "failed to fetch goroutine pprof from nodeA-1")
	require.Contains(t, out, "failed to fetch goroutine pprof from 1 node(s): nodeB-2")
}

func TestGetPprofCommandTreatsNonOKStatusAsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("<html>error page</html>"))
	}))
	defer srv.Close()

	_, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.ParseInt(portStr, 10, 64)
	require.NoError(t, err)

	session := &models.Session{ServerID: 1, ServerName: "nodeA", Address: fmt.Sprintf("127.0.0.1:%d", port)}

	s := &InstanceState{
		client:   &pprofTestKV{sessions: []*models.Session{session}},
		basePath: "by-dev/meta",
	}

	dir := t.TempDir()
	t.Chdir(dir)

	var cmdErr error
	out := captureStdout(t, func() {
		cmdErr = s.GetPprofCommand(context.Background(), &PprofParam{Type: "goroutine", Port: port})
	})
	require.NoError(t, cmdErr)

	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 1)

	entries := readTarEntries(t, files[0].Name())
	require.Empty(t, entries, "a non-2xx response must not be archived as real profile data")

	require.Contains(t, out, "unexpected status code 500")
	require.Contains(t, out, "failed to fetch goroutine pprof from 1 node(s): nodeA-1")
}

func TestGetPprofCommandTimesOutOnHungNode(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	old := pprofFetchTimeout
	pprofFetchTimeout = 200 * time.Millisecond
	defer func() { pprofFetchTimeout = old }()

	_, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.ParseInt(portStr, 10, 64)
	require.NoError(t, err)

	session := &models.Session{ServerID: 1, ServerName: "nodeA", Address: fmt.Sprintf("127.0.0.1:%d", port)}
	s := &InstanceState{
		client:   &pprofTestKV{sessions: []*models.Session{session}},
		basePath: "by-dev/meta",
	}
	t.Chdir(t.TempDir())

	var cmdErr error
	done := make(chan string, 1)
	go func() {
		done <- captureStdout(t, func() {
			cmdErr = s.GetPprofCommand(context.Background(), &PprofParam{Type: "goroutine", Port: port})
		})
	}()

	select {
	case out := <-done:
		require.NoError(t, cmdErr)
		require.Contains(t, out, "failed to fetch goroutine pprof from nodeA-1")
	case <-time.After(10 * time.Second):
		t.Fatal("GetPprofCommand did not return for a hung node")
	}
}
