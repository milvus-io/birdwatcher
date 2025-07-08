package storage

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type EntryFilter interface {
	Match(pk common.PrimaryKey, ts int64, values map[int64]any) (bool, error)
}

var _ EntryFilter = (*DeltalogFilter)(nil)

type DeltalogFilter struct {
	deleteEntries map[any]uint64
}

func (f *DeltalogFilter) Match(pk common.PrimaryKey, ts int64, values map[int64]any) (bool, error) {
	return f.deleteEntries[pk.GetValue()] <= uint64(ts), nil
}

func NewDeltalogFilter(entries map[any]uint64) *DeltalogFilter {
	return &DeltalogFilter{
		deleteEntries: entries,
	}
}

var _ EntryFilter = (*ExprFilter)(nil)

type ExprFilter struct {
	id2Schema map[int64]*schemapb.FieldSchema
	expr      string
	program   *vm.Program
}

func (f *ExprFilter) Match(pk common.PrimaryKey, ts int64, values map[int64]any) (bool, error) {
	pkv := pk.GetValue()
	env := lo.MapKeys(values, func(_ any, fid int64) string {
		return f.id2Schema[fid].Name
	})
	env["$pk"] = pkv
	env["$timestamp"] = ts

	output, err := expr.Run(f.program, env)
	if err != nil {
		return false, err
	}

	match, ok := output.(bool)
	if !ok {
		fmt.Printf("filter expression result not bool but %T\n", output)
		return false, errors.Newf("filter expression result not bool, actual result: %v", output)
	}

	return match, nil
}

func NewExprFilter(id2Schema map[int64]*schemapb.FieldSchema, iexpr string) (*ExprFilter, error) {
	program, err := expr.Compile(iexpr)
	if err != nil {
		return nil, err
	}
	return &ExprFilter{
		id2Schema: id2Schema,
		expr:      iexpr,
		program:   program,
	}, nil
}
