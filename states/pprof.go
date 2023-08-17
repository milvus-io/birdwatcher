package states

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type PprofParam struct {
	framework.ParamBase `use:"pprof" desc:"get pprof from online components"`
	Type                string `name:"type" default:"goroutine" desc:"pprof metric type to fetch"`
	Port                int64  `name:"port" default:"9091" desc:"metrics port milvus component is using"`
}

func (s *InstanceState) GetPprofCommand(ctx context.Context, p *PprofParam) error {

	switch p.Type {
	case "goroutine", "heap", "profile", "allocs":
	default:
		return errors.New("invalid pprof metric type provided")
	}

	sessions, err := common.ListSessions(s.client, s.basePath)
	if err != nil {
		return errors.Wrap(err, "failed to list sessions")
	}

	now := time.Now()
	filePath := fmt.Sprintf("bw_pprof_%s.%s.tar.gz", p.Type, now.Format("060102-150405"))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
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

	for _, session := range sessions {
		addr := session.IP()
		// TODO add auto detection from configuration API
		url := fmt.Sprintf("http://%s:%d/debug/pprof/%s?debug=0", addr, p.Port, p.Type)

		// #nosec
		resp, err := http.Get(url)
		if err != nil {
			return err
		}

		bs, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,

			Name: fmt.Sprintf("%s_%d_%s", session.ServerName, session.ServerID, p.Type),
			Size: int64(len(bs)),
			Mode: 0600,
		})

		_, err = tw.Write(bs)
		if err != nil {
			return err
		}

		fmt.Printf("%s pprof from %s-%d fetched, added into archive file\n", p.Type, session.ServerName, session.ServerID)
	}

	fmt.Printf("pprof metrics fetch done, write to archive file %s\n", filePath)

	return nil
}
