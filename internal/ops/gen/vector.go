package gen

import (
	"fmt"
	"math/rand"
)

type randomFloatVector struct{}

func (randomFloatVector) Generate(spec *Spec, r *rand.Rand) (any, error) {
	count, err := requireCount(spec)
	if err != nil {
		return nil, err
	}
	dim := argInt(spec.Args, "dim", 0)
	if dim <= 0 {
		return nil, fmt.Errorf("random_float_vector: `dim` must be > 0")
	}
	out := make([][]float32, count)
	for i := 0; i < count; i++ {
		v := make([]float32, dim)
		for j := 0; j < dim; j++ {
			v[j] = r.Float32()
		}
		out[i] = v
	}
	return out, nil
}

type randomBinaryVector struct{}

func (randomBinaryVector) Generate(spec *Spec, r *rand.Rand) (any, error) {
	count, err := requireCount(spec)
	if err != nil {
		return nil, err
	}
	dim := argInt(spec.Args, "dim", 0)
	if dim <= 0 || dim%8 != 0 {
		return nil, fmt.Errorf("random_binary_vector: `dim` must be a positive multiple of 8")
	}
	bytes := dim / 8
	out := make([][]byte, count)
	for i := 0; i < count; i++ {
		b := make([]byte, bytes)
		r.Read(b)
		out[i] = b
	}
	return out, nil
}

func init() {
	Register("random_float_vector", randomFloatVector{})
	Register("random_binary_vector", randomBinaryVector{})
}
