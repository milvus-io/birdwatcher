package states

import (
	"archive/tar"
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/models"
)

func TestFetchPprofData(t *testing.T) {
	t.Run("2xx returns the body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("REAL-GOROUTINE-DUMP-DATA"))
		}))
		defer srv.Close()

		data, err := fetchPprofData(srv.URL)
		require.NoError(t, err)
		require.Equal(t, "REAL-GOROUTINE-DUMP-DATA", string(data))
	})

	t.Run("non-2xx is reported as an error, not archived", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("<html>error page</html>"))
		}))
		defer srv.Close()

		data, err := fetchPprofData(srv.URL)
		require.Error(t, err)
		require.Nil(t, data)
	})

	t.Run("unreachable node is reported as an error", func(t *testing.T) {
		// Port 1 is not expected to have anything listening.
		data, err := fetchPprofData("http://127.0.0.1:1/debug/pprof/goroutine?debug=0")
		require.Error(t, err)
		require.Nil(t, data)
	})
}

func TestWritePprofArchiveEntry(t *testing.T) {
	sessions := []*models.Session{{ServerName: "nodeA", ServerID: 1}}

	t.Run("successful fetch is written to the archive", func(t *testing.T) {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)

		skipped, err := writePprofArchiveEntry(tw, pprofResult{
			sessions: sessions,
			data:     []byte("goroutine dump"),
		}, "goroutine")
		require.NoError(t, err)
		require.False(t, skipped)
		require.NoError(t, tw.Close())
		require.Equal(t, 1, countTarEntries(t, buf.Bytes()), "a successful fetch must produce one archive entry")
	})

	t.Run("failed fetch is skipped and leaves no 0-byte entry", func(t *testing.T) {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)

		skipped, err := writePprofArchiveEntry(tw, pprofResult{
			sessions: sessions,
			err:      errors.New("connection refused"),
		}, "goroutine")
		require.Error(t, err)
		require.True(t, skipped, "a failed fetch must be skipped, not archived")
		require.NoError(t, tw.Close())
		require.Equal(t, 0, countTarEntries(t, buf.Bytes()),
			"a failed fetch must not leave a 0-byte archive entry")
	})
}

// countTarEntries reads back a tar archive and counts its file entries.
func countTarEntries(t *testing.T, raw []byte) int {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(raw))
	count := 0
	for {
		_, err := tr.Next()
		if err == io.EOF {
			return count
		}
		require.NoError(t, err)
		count++
	}
}
