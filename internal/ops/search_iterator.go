package ops

import "context"

// SearchIteratorParams is a v2 stub.
type SearchIteratorParams struct{}

func (p *SearchIteratorParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	return nil, ErrNotImplemented
}

func init() {
	Register("search_iterator", func() Op { return &SearchIteratorParams{} })
}
