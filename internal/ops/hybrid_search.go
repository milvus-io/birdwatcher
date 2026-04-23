package ops

import "context"

// HybridSearchParams is a v2 stub — registered so scenarios referencing it
// fail fast with a clear message rather than "unknown op".
type HybridSearchParams struct{}

func (p *HybridSearchParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	return nil, ErrNotImplemented
}

func init() {
	Register("hybrid_search", func() Op { return &HybridSearchParams{} })
}
