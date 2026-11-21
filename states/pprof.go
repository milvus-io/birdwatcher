package states

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

// defaultPprofFetchTimeout bounds each per-node fetch so a node that accepts
// the connection but never responds cannot hang the command. It must exceed
// the 30s default duration of the "profile" type.
const defaultPprofFetchTimeout = 2 * time.Minute

// pprofFetchOptions carries the knobs of a pprof fetch that callers (tests in
// particular) need to control without touching process-wide state such as
// the working directory, os.Stdout or package-level variables.
type pprofFetchOptions struct {
	// outputDir is the directory the archive is written to; empty means the
	// current working directory.
	outputDir string
	// out receives progress and failure messages.
	out io.Writer
	// timeout bounds each per-node fetch.
	timeout time.Duration
}

type PprofParam struct {
	framework.ParamBase `use:"pprof" desc:"get pprof from online components"`
	Type                string `name:"type" default:"goroutine" desc:"pprof metric type to fetch"`
	Port                int64  `name:"port" default:"9091" desc:"metrics port milvus component is using"`
}

func (s *InstanceState) GetPprofCommand(ctx context.Context, p *PprofParam) error {
	return s.fetchPprof(ctx, p, pprofFetchOptions{
		out:     os.Stdout,
		timeout: defaultPprofFetchTimeout,
	})
}

func (s *InstanceState) fetchPprof(ctx context.Context, p *PprofParam, opts pprofFetchOptions) error {
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
	filePath := filepath.Join(opts.outputDir, fmt.Sprintf("bw_pprof_%s.%s.tar.gz", p.Type, now.Format("060102-150405")))
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

	type pprofResult struct {
		id       int64
		sessions []*models.Session
		data     []byte
		err      error
	}

	ch := make(chan pprofResult, len(sessions))
	// writeErrCh carries the single slice of archive-write errors collected
	// by the consumer goroutine below; it is sent to exactly once, so its
	// buffer of 1 can never block regardless of how many errors occur.
	writeErrCh := make(chan []error, 1)

	var failedNodes []string

	go func() {
		var writeErrs []error
		for result := range ch {
			session := result.sessions[0]
			serverName := session.ServerName
			// set to mixture if there are multiple sessions in group
			if len(result.sessions) > 1 {
				serverName = "mixture"
			}

			if result.err != nil {
				fmt.Fprintf(opts.out, "failed to fetch %s pprof from %s-%d: %s\n", p.Type, serverName, session.ServerID, result.err.Error())
				failedNodes = append(failedNodes, fmt.Sprintf("%s-%d", serverName, session.ServerID))
				continue
			}

			err := tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg,

				Name: fmt.Sprintf("%s_%d_%s", serverName, session.ServerID, p.Type),
				Size: int64(len(result.data)),
				Mode: 0o600,
			})
			if err != nil {
				writeErrs = append(writeErrs, err)
				continue
			}

			_, err = tw.Write(result.data)
			if err != nil {
				writeErrs = append(writeErrs, err)
				continue
			}

			fmt.Fprintf(opts.out, "%s pprof from %s-%d fetched, added into archive file\n", p.Type, serverName, session.ServerID)
		}
		writeErrCh <- writeErrs
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

			reqCtx, cancel := context.WithTimeout(ctx, opts.timeout)
			defer cancel()
			req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
			if err != nil {
				result.err = err
				ch <- result
				return
			}

			// #nosec
			resp, err := (&http.Client{Timeout: opts.timeout}).Do(req)
			if err != nil {
				result.err = err
				ch <- result
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				result.err = errors.Newf("unexpected status code %d from %s", resp.StatusCode, url)
				ch <- result
				return
			}

			bs, err := io.ReadAll(resp.Body)
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

	for _, err := range <-writeErrCh {
		fmt.Fprintln(opts.out, "failed to write pprof:", err.Error())
	}

	fmt.Fprintf(opts.out, "pprof metrics fetch done, write to archive file %s\n", filePath)
	if len(failedNodes) > 0 {
		fmt.Fprintf(opts.out, "failed to fetch %s pprof from %d node(s): %s\n", p.Type, len(failedNodes), strings.Join(failedNodes, ", "))
	}

	return nil
}
