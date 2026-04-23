package gen

import (
	"fmt"
	"math/rand"
)

type rangeInt64 struct{}

func (rangeInt64) Generate(spec *Spec, r *rand.Rand) (any, error) {
	count, err := requireCount(spec)
	if err != nil {
		return nil, err
	}
	start := argInt64(spec.Args, "start", 0)
	step := argInt64(spec.Args, "step", 1)
	out := make([]int64, count)
	for i := 0; i < count; i++ {
		out[i] = start + int64(i)*step
	}
	return out, nil
}

type randomInt64 struct{}

func (randomInt64) Generate(spec *Spec, r *rand.Rand) (any, error) {
	count, err := requireCount(spec)
	if err != nil {
		return nil, err
	}
	min := argInt64(spec.Args, "min", 0)
	max := argInt64(spec.Args, "max", 1<<30)
	if max <= min {
		return nil, fmt.Errorf("random_int64: `max` must be > `min`")
	}
	span := max - min
	out := make([]int64, count)
	for i := 0; i < count; i++ {
		out[i] = min + r.Int63n(span)
	}
	return out, nil
}

type randomFloat struct{}

func (randomFloat) Generate(spec *Spec, r *rand.Rand) (any, error) {
	count, err := requireCount(spec)
	if err != nil {
		return nil, err
	}
	min := argFloat64(spec.Args, "min", 0)
	max := argFloat64(spec.Args, "max", 1)
	if max <= min {
		return nil, fmt.Errorf("random_float: `max` must be > `min`")
	}
	span := float32(max - min)
	mn := float32(min)
	out := make([]float32, count)
	for i := 0; i < count; i++ {
		out[i] = mn + r.Float32()*span
	}
	return out, nil
}

var defaultAlphabet = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

type randomVarchar struct{}

func (randomVarchar) Generate(spec *Spec, r *rand.Rand) (any, error) {
	count, err := requireCount(spec)
	if err != nil {
		return nil, err
	}
	length := argInt(spec.Args, "length", 16)
	if length <= 0 {
		return nil, fmt.Errorf("random_varchar: `length` must be > 0")
	}
	alpha := defaultAlphabet
	if s := argString(spec.Args, "charset", ""); s != "" {
		alpha = []rune(s)
	}
	out := make([]string, count)
	buf := make([]rune, length)
	for i := 0; i < count; i++ {
		for j := 0; j < length; j++ {
			buf[j] = alpha[r.Intn(len(alpha))]
		}
		out[i] = string(buf)
	}
	return out, nil
}

type randomBool struct{}

func (randomBool) Generate(spec *Spec, r *rand.Rand) (any, error) {
	count, err := requireCount(spec)
	if err != nil {
		return nil, err
	}
	out := make([]bool, count)
	for i := 0; i < count; i++ {
		out[i] = r.Intn(2) == 1
	}
	return out, nil
}

// constGen produces a slice of repeated values. The value's Go type
// (int64 / float64 / bool / string) is preserved by YAML decoding.
type constGen struct{}

func (constGen) Generate(spec *Spec, r *rand.Rand) (any, error) {
	count, err := requireCount(spec)
	if err != nil {
		return nil, err
	}
	raw, ok := spec.Args["value"]
	if !ok {
		return nil, fmt.Errorf("const: `value` is required")
	}
	switch v := raw.(type) {
	case int:
		out := make([]int64, count)
		for i := range out {
			out[i] = int64(v)
		}
		return out, nil
	case int64:
		out := make([]int64, count)
		for i := range out {
			out[i] = v
		}
		return out, nil
	case float64:
		out := make([]float32, count)
		for i := range out {
			out[i] = float32(v)
		}
		return out, nil
	case bool:
		out := make([]bool, count)
		for i := range out {
			out[i] = v
		}
		return out, nil
	case string:
		out := make([]string, count)
		for i := range out {
			out[i] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("const: unsupported value type %T", raw)
}

func init() {
	Register("range_int64", rangeInt64{})
	Register("random_int64", randomInt64{})
	Register("random_float", randomFloat{})
	Register("random_varchar", randomVarchar{})
	Register("random_bool", randomBool{})
	Register("const", constGen{})
}
