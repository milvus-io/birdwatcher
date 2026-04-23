package ops

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// IndexSpec describes which index to build. `Type` is the ANN index name
// (HNSW / IVF_FLAT / IVF_PQ / FLAT / AUTOINDEX / DISKANN). `Params` carries
// index-specific knobs (M, efConstruction, nlist, m, nbits, ...) and is
// loosely typed because different index types accept different keys.
type IndexSpec struct {
	Type   string         `yaml:"type"`
	Metric string         `yaml:"metric"`
	Name   string         `yaml:"name,omitempty"`
	Params map[string]any `yaml:"params,omitempty"`
}

type CreateIndexParams struct {
	Collection string    `yaml:"collection"`
	Field      string    `yaml:"field"`
	Index      IndexSpec `yaml:"index"`
	Async      bool      `yaml:"async,omitempty"`
}

func (p *CreateIndexParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("create_index")
	}
	if p.Collection == "" || p.Field == "" {
		return nil, fmt.Errorf("create_index: `collection` and `field` required")
	}
	idx, err := buildIndex(p.Index)
	if err != nil {
		return nil, err
	}
	opt := milvusclient.NewCreateIndexOption(p.Collection, p.Field, idx)
	if p.Index.Name != "" {
		opt = opt.WithIndexName(p.Index.Name)
	}
	task, err := rc.Client.CreateIndex(ctx, opt)
	if err != nil {
		return nil, err
	}
	if !p.Async {
		if err := task.Await(ctx); err != nil {
			return nil, err
		}
	}
	fmt.Fprintf(rc.Out(), "index created on %s.%s\n", p.Collection, p.Field)
	return map[string]any{"collection": p.Collection, "field": p.Field}, nil
}

// buildIndex maps the string index type to the concrete index constructor.
// Unknown types fall through to AUTOINDEX with a warning-style error so the
// caller sees it rather than getting silent wrong behavior.
func buildIndex(spec IndexSpec) (index.Index, error) {
	metric, err := parseMetric(spec.Metric)
	if err != nil {
		return nil, err
	}
	t := strings.ToUpper(strings.TrimSpace(spec.Type))
	switch t {
	case "", "AUTOINDEX", "AUTO":
		return index.NewAutoIndex(metric), nil
	case "FLAT":
		return index.NewFlatIndex(metric), nil
	case "BIN_FLAT", "BINFLAT":
		return index.NewBinFlatIndex(metric), nil
	case "IVF_FLAT", "IVFFLAT":
		nlist := intParam(spec.Params, "nlist", 128)
		return index.NewIvfFlatIndex(metric, nlist), nil
	case "IVF_PQ", "IVFPQ":
		nlist := intParam(spec.Params, "nlist", 128)
		m := intParam(spec.Params, "m", 8)
		nbits := intParam(spec.Params, "nbits", 8)
		return index.NewIvfPQIndex(metric, nlist, m, nbits), nil
	case "HNSW":
		m := intParam(spec.Params, "M", intParam(spec.Params, "m", 16))
		ef := intParam(spec.Params, "efConstruction", intParam(spec.Params, "ef_construction", 200))
		return index.NewHNSWIndex(metric, m, ef), nil
	case "DISKANN", "DISK_ANN":
		return index.NewDiskANNIndex(metric), nil
	case "SPARSE_INVERTED_INDEX", "SPARSE":
		drop := floatParam(spec.Params, "drop_ratio_build", 0)
		return index.NewSparseInvertedIndex(metric, drop), nil
	}
	return nil, fmt.Errorf("unsupported index type %q", spec.Type)
}

func parseMetric(s string) (entity.MetricType, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "", "L2":
		return entity.L2, nil
	case "IP":
		return entity.IP, nil
	case "COSINE":
		return entity.COSINE, nil
	case "HAMMING":
		return entity.HAMMING, nil
	case "JACCARD":
		return entity.JACCARD, nil
	case "BM25":
		return entity.BM25, nil
	}
	return "", fmt.Errorf("unsupported metric %q", s)
}

func intParam(m map[string]any, key string, def int) int {
	v, ok := m[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return def
}

func floatParam(m map[string]any, key string, def float64) float64 {
	v, ok := m[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return def
}

func init() {
	Register("create_index", func() Op { return &CreateIndexParams{} })
}
