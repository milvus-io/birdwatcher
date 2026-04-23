package milvusctl

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/internal/ops"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// -----------------------------------------------------------------------------
// datacoord GC pause / resume

type GcPauseParam struct {
	framework.ParamBase `use:"gc pause" desc:"pause datacoord garbage collection for a duration"`
	Seconds             int64  `name:"seconds" default:"60" desc:"pause duration in seconds"`
	CollectionID        int64  `name:"collection-id" default:"0" desc:"optional collection id to scope the pause"`
	Addr                string `name:"addr" default:"" desc:"proxy management HTTP address; defaults to deriving from current connection"`
	APIKey              string `name:"apikey" default:"" desc:"optional API key, sent as Bearer token"`
}

func (s *MilvusctlState) GcPauseCommand(ctx context.Context, p *GcPauseParam) error {
	addr, err := s.resolveManagementAddr(p.Addr)
	if err != nil {
		return err
	}
	apiKey := p.APIKey
	if apiKey == "" {
		apiKey = s.clientcfg.APIKey
	}
	return s.executeOp(ctx, &ops.GcPauseParams{
		Addr:         addr,
		APIKey:       apiKey,
		PauseSeconds: p.Seconds,
		CollectionID: p.CollectionID,
	})
}

type GcResumeParam struct {
	framework.ParamBase `use:"gc resume" desc:"resume datacoord garbage collection"`
	Ticket              string `name:"ticket" default:"" desc:"ticket returned by gc pause (optional for backward compat)"`
	Addr                string `name:"addr" default:"" desc:"proxy management HTTP address; defaults to deriving from current connection"`
	APIKey              string `name:"apikey" default:"" desc:"optional API key, sent as Bearer token"`
}

func (s *MilvusctlState) GcResumeCommand(ctx context.Context, p *GcResumeParam) error {
	addr, err := s.resolveManagementAddr(p.Addr)
	if err != nil {
		return err
	}
	apiKey := p.APIKey
	if apiKey == "" {
		apiKey = s.clientcfg.APIKey
	}
	return s.executeOp(ctx, &ops.GcResumeParams{
		Addr:   addr,
		APIKey: apiKey,
		Ticket: p.Ticket,
	})
}

// resolveManagementAddr returns the explicit addr if given, otherwise
// derives one from the current client's SDK address.
func (s *MilvusctlState) resolveManagementAddr(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	addr, err := deriveManagementAddr(s.clientcfg.Address)
	if err != nil {
		return "", fmt.Errorf("failed to derive management address, pass --addr: %w", err)
	}
	return addr, nil
}

// deriveManagementAddr converts an SDK address (e.g. host:19530 or
// http://host:19530) into the proxy management HTTP URL on port 9091.
func deriveManagementAddr(sdkAddr string) (string, error) {
	if sdkAddr == "" {
		return "", fmt.Errorf("empty connection address")
	}
	raw := sdkAddr
	scheme := "http"
	if rest, ok := strings.CutPrefix(raw, "https://"); ok {
		scheme = "https"
		raw = rest
	} else if rest, ok := strings.CutPrefix(raw, "http://"); ok {
		raw = rest
	}
	raw = strings.TrimSuffix(raw, "/")
	host := raw
	if i := strings.LastIndex(raw, ":"); i >= 0 {
		host = raw[:i]
	}
	if host == "" {
		return "", fmt.Errorf("cannot parse host from %q", sdkAddr)
	}
	return fmt.Sprintf("%s://%s:9091", scheme, host), nil
}

// -----------------------------------------------------------------------------
// use database

type UseDatabaseParam struct {
	framework.ParamBase `use:"use database" desc:"switch active database"`
	DB                  string `name:"db" default:"" desc:"database name"`
}

func (s *MilvusctlState) UseDatabaseCommand(ctx context.Context, p *UseDatabaseParam) error {
	if p.DB == "" {
		return fmt.Errorf("--db is required")
	}
	if err := s.client.UseDatabase(ctx, milvusclient.NewUseDatabaseOption(p.DB)); err != nil {
		return err
	}
	s.clientcfg.DBName = p.DB
	s.SetLabel(fmt.Sprintf("Milvusctl[%s]", s.endpoint()))
	fmt.Printf("using database %q\n", p.DB)
	return nil
}
