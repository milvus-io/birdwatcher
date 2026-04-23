package ops

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/internal/ops/gen"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// ColumnSpec names one column of an Insert/Upsert request. Exactly one of
// `Gen` or `Data` must be set — `Gen` delegates to a registered generator
// (`random_float_vector`, `range_int64`, ...), `Data` lets callers pass
// literal slices (used when ops are constructed from Go code directly).
type ColumnSpec struct {
	Field string    `yaml:"field"`
	Gen   *gen.Spec `yaml:"gen,omitempty"`
	Data  any       `yaml:"data,omitempty"`
}

type InsertParams struct {
	Collection string       `yaml:"collection"`
	Partition  string       `yaml:"partition,omitempty"`
	Columns    []ColumnSpec `yaml:"columns"`
}

type InsertResult struct {
	Count int64 `yaml:"count"`
}

func (p *InsertParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("insert")
	}
	if p.Collection == "" {
		return nil, fmt.Errorf("insert: `collection` required")
	}
	if len(p.Columns) == 0 {
		return nil, fmt.Errorf("insert: at least one column required")
	}
	cols, err := buildColumns(p.Columns, rc)
	if err != nil {
		return nil, err
	}
	opt := milvusclient.NewColumnBasedInsertOption(p.Collection, cols...)
	if p.Partition != "" {
		opt = opt.WithPartition(p.Partition)
	}
	res, err := rc.Client.Insert(ctx, opt)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(rc.Out(), "inserted %d rows into %q\n", res.InsertCount, p.Collection)
	return InsertResult{Count: res.InsertCount}, nil
}

// buildColumns resolves each ColumnSpec (generator or literal) into a
// typed column.Column. Used by both Insert and Upsert.
func buildColumns(specs []ColumnSpec, rc *RunContext) ([]column.Column, error) {
	out := make([]column.Column, 0, len(specs))
	for _, c := range specs {
		data, err := resolveColumnData(c, rc)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", c.Field, err)
		}
		col, err := makeColumn(c.Field, data)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", c.Field, err)
		}
		out = append(out, col)
	}
	return out, nil
}

func resolveColumnData(c ColumnSpec, rc *RunContext) (any, error) {
	if c.Gen != nil {
		return gen.Resolve(c.Gen, rc.Rand)
	}
	if c.Data != nil {
		return coerceLiteral(c.Data)
	}
	return nil, fmt.Errorf("neither `gen` nor `data` set")
}

// coerceLiteral turns the loose []interface{} / interface{} produced by
// YAML decoding into the typed slice that makeColumn dispatches on.
// Typed slices (already []int64, []string, [][]float32, ...) pass through
// untouched so Go callers aren't affected.
func coerceLiteral(data any) (any, error) {
	raw, ok := data.([]any)
	if !ok {
		return data, nil
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty `data` slice; specify at least one element or use `gen`")
	}
	switch raw[0].(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		out := make([]int64, len(raw))
		for i, v := range raw {
			n, err := toInt64(v)
			if err != nil {
				return nil, fmt.Errorf("data[%d]: %w", i, err)
			}
			out[i] = n
		}
		return out, nil
	case float32, float64:
		out := make([]float32, len(raw))
		for i, v := range raw {
			f, err := toFloat64(v)
			if err != nil {
				return nil, fmt.Errorf("data[%d]: %w", i, err)
			}
			out[i] = float32(f)
		}
		return out, nil
	case bool:
		out := make([]bool, len(raw))
		for i, v := range raw {
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("data[%d]: mixed types, expected bool got %T", i, v)
			}
			out[i] = b
		}
		return out, nil
	case string:
		out := make([]string, len(raw))
		for i, v := range raw {
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("data[%d]: mixed types, expected string got %T", i, v)
			}
			out[i] = s
		}
		return out, nil
	case []any:
		// nested slice → float vector literal, e.g. [[0.1, 0.2], [0.3, 0.4]]
		out := make([][]float32, len(raw))
		for i, v := range raw {
			inner, ok := v.([]any)
			if !ok {
				return nil, fmt.Errorf("data[%d]: mixed types, expected []any got %T", i, v)
			}
			row := make([]float32, len(inner))
			for j, e := range inner {
				f, err := toFloat64(e)
				if err != nil {
					return nil, fmt.Errorf("data[%d][%d]: %w", i, j, err)
				}
				row[j] = float32(f)
			}
			out[i] = row
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported literal element type %T", raw[0])
}

func toInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int8:
		return int64(n), nil
	case int16:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	case uint:
		return int64(n), nil
	case uint8:
		return int64(n), nil
	case uint16:
		return int64(n), nil
	case uint32:
		return int64(n), nil
	case uint64:
		return int64(n), nil
	}
	return 0, fmt.Errorf("mixed types, expected int got %T", v)
}

func toFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case float32:
		return float64(n), nil
	case float64:
		return n, nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	}
	return 0, fmt.Errorf("mixed types, expected float got %T", v)
}

// makeColumn dispatches by Go slice type. Keep in sync with gen outputs.
func makeColumn(field string, data any) (column.Column, error) {
	switch v := data.(type) {
	case []int64:
		return column.NewColumnInt64(field, v), nil
	case []int32:
		return column.NewColumnInt32(field, v), nil
	case []int16:
		return column.NewColumnInt16(field, v), nil
	case []int8:
		return column.NewColumnInt8(field, v), nil
	case []float32:
		return column.NewColumnFloat(field, v), nil
	case []float64:
		return column.NewColumnDouble(field, v), nil
	case []bool:
		return column.NewColumnBool(field, v), nil
	case []string:
		return column.NewColumnVarChar(field, v), nil
	case [][]float32:
		if len(v) == 0 {
			return nil, fmt.Errorf("empty float vector column")
		}
		return column.NewColumnFloatVector(field, len(v[0]), v), nil
	case [][]byte:
		if len(v) == 0 {
			return nil, fmt.Errorf("empty binary vector column")
		}
		return column.NewColumnBinaryVector(field, len(v[0])*8, v), nil
	case [][]int8:
		if len(v) == 0 {
			return nil, fmt.Errorf("empty int8 vector column")
		}
		return column.NewColumnInt8Vector(field, len(v[0]), v), nil
	}
	return nil, fmt.Errorf("unsupported column data type %T", data)
}

func init() {
	Register("insert", func() Op { return &InsertParams{} })
}
