package framework

import "encoding/json"

type Format int32

const (
	FormatDefault Format = iota + 1
	FormatPlain
	FormatJSON
	FormatTable
	FormatLine
)

var name2Format = map[string]Format{
	"default": FormatDefault,
	"plain":   FormatPlain,
	"json":    FormatJSON,
	"table":   FormatTable,
	"line":    FormatLine,
}

// ResultSet is the interface for command result set.
type ResultSet interface {
	PrintAs(Format) string
	Entities() any
}

// PresetResultSet implements Stringer and "memorize" output format.
type PresetResultSet struct {
	ResultSet
	format Format
}

func (rs *PresetResultSet) String() string {
	if rs.format < FormatDefault {
		return rs.PrintAs(FormatDefault)
	}
	return rs.PrintAs(rs.format)
}

func NewPresetResultSet(rs ResultSet, format Format) *PresetResultSet {
	return &PresetResultSet{
		ResultSet: rs,
		format:    format,
	}
}

// NameFormat name to format mapping tool function.
func NameFormat(name string) Format {
	f, ok := name2Format[name]
	if !ok {
		return FormatDefault
	}
	return f
}

type ListResultSet[T any] struct {
	Data []T
}

func (rs *ListResultSet[T]) Entities() any {
	return rs.Data
}

func (rs *ListResultSet[T]) SetData(data []T) {
	rs.Data = data
}

func NewListResult[LRS any, P interface {
	*LRS
	SetData([]E)
}, E any](data []E) *LRS {
	var t LRS
	var p P = &t
	p.SetData(data)
	return &t
}

// MarshalJSON is a helper function for JSON serialization.
// It returns a pretty-printed JSON string of the given value.
func MarshalJSON(v any) string {
	bs, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(bs)
}
