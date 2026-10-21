package states

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type PprofParam struct {
	framework.ParamBase `use:"pprof" desc:"get pprof from online components"`
	Type                string `name:"type" default:"goroutine" desc:"pprof metric type to fetch"`
	Port                int64  `name:"port" default:"9091" desc:"metrics port milvus component is using"`
}

// pprofResult carries one node's fetch outcome to the archive writer.
type pprofResult struct {
	id       int64
	sessions []*models.Session
	data     []byte
	err      error
}

// fetchPprofData performs a single pprof HTTP GET. It enforces a 2xx status and
// always closes the response body, so a non-2xx response (an error page served
// with 200 is not the concern here; a 404/500 is) is reported as an error
// instead of being archived as if it were real profile data, and the
// underlying connection is never leaked.
func fetchPprofData(url string) ([]byte, error) {
	// #nosec
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, errors.Errorf("unexpected HTTP status %d from %s", resp.StatusCode, url)
	}

	return io.ReadAll(resp.Body)
}

// writePprofArchiveEntry writes a successful fetch into the tar archive. It
// returns skipped=true and writes nothing when the fetch failed, so a failed
// node never leaves a 0-byte entry that is indistinguishable from a genuine
// empty response.
func writePprofArchiveEntry(tw *tar.Writer, result pprofResult, pprofType string) (skipped bool, err error) {
	if result.err != nil {
		return true, result.err
	}

	session := result.sessions[0]
	serverName := session.ServerName
	// set to mixture if there are multiple sessions in group
	if len(result.sessions) > 1 {
		serverName = "mixture"
	}

	hdr := &tar.Header{
		Typeflag: tar.TypeReg,
		Name:     fmt.Sprintf("%s_%d_%s", serverName, session.ServerID, pprofType),
		Size:     int64(len(result.data)),
		Mode:     0o600,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return false, err
	}
	if _, err := tw.Write(result.data); err != nil {
		return false, err
	}
	return false, nil
}

func (s *InstanceState) GetPprofCommand(ctx context.Context, p *PprofParam) error {
	switch p.Type {
	case "goroutine", "heap", "profile", "allocs", "block", "mutex":
	default:
		return errors.New("invalid pprof metric type provided")
	}

	sessions, err := common.ListSessions(ctx, s.client, s.basePath)
	if err != nil {
		return errors.Wrap(err, "failed to list sessions")
	}

	now := time.Now()
	filePath := fmt.Sprintf("bw_pprof_%s.%s.tar.gz", p.Type, now.Format("060102-150405"))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	// Create new Writers for gzip and tar
	// These writers are chained. Writing to the tar writer will
	// write to the gzip writer which in turn will write to
	// the "buf" writer
	gw := gzip.NewWriter(f)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	// dedup by serverID, standalone sessions shares same serverID
	groups := lo.GroupBy(sessions, func(session *models.Session) int64 {
		return session.ServerID
	})

	ch := make(chan pprofResult, len(sessions))
	signal := make(chan error, 1)

	// failed collects the identities of nodes whose fetch failed, so the
	// command can print a final summary instead of silently discarding them.
	var failed []string

	go func() {
		for result := range ch {
			session := result.sessions[0]
			serverName := session.ServerName
			if len(result.sessions) > 1 {
				serverName = "mixture"
			}
			nodeID := fmt.Sprintf("%s-%d", serverName, session.ServerID)

			// A failed fetch must not be archived: skip the entry, report the
			// real error with the node's identity, and never print the
			// "fetched, added into archive file" success line for it.
			skipped, werr := writePprofArchiveEntry(tw, result, p.Type)
			if skipped {
				fmt.Printf("failed to fetch %s pprof from %s: %v\n", p.Type, nodeID, werr)
				failed = append(failed, nodeID)
				continue
			}
			if werr != nil {
				signal <- werr
				continue
			}

			fmt.Printf("%s pprof from %s fetched, added into archive file\n", p.Type, nodeID)
		}
		close(signal)
	}()

	wg := sync.WaitGroup{}
	wg.Add(len(groups))
	for id, sessions := range groups {
		go func(id int64, sessions []*models.Session) {
			defer wg.Done()
			// protection logic
			if len(sessions) == 0 {
				return
			}

			result := pprofResult{
				sessions: sessions,
			}
			addr := sessions[0].IP()
			// TODO add auto detection from configuration API
			url := fmt.Sprintf("http://%s:%d/debug/pprof/%s?debug=0", addr, p.Port, p.Type)

			bs, err := fetchPprofData(url)
			if err != nil {
				result.err = err
				ch <- result
				return
			}

			result.data = bs
			ch <- result
		}(id, sessions)
	}
	wg.Wait()
	close(ch)

	for err := range signal {
		if err != nil {
			fmt.Println("failed to write pprof:", err.Error())
		}
	}

	fmt.Printf("pprof metrics fetch done, write to archive file %s\n", filePath)

	if len(failed) > 0 {
		return errors.Errorf("pprof fetch failed for %d node(s), skipped in archive: %s",
			len(failed), strings.Join(failed, ", "))
	}

	return nil
}
