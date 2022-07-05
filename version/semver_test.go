package version

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestSemVer_Encoding(t *testing.T) {
	v := &SemVer{
		Major:      1,
		Minor:      2,
		Patch:      3,
		PreRelease: "RC1",
		Build:      "57db51c",
	}

	bs, err := proto.Marshal(v)
	assert.NoError(t, err)

	v.Reset()

	err = proto.Unmarshal(bs, v)
	assert.NoError(t, err)

	assert.EqualValues(t, 1, v.Major)
	assert.EqualValues(t, 2, v.Minor)
	assert.EqualValues(t, 3, v.Patch)
	assert.EqualValues(t, "RC1", v.PreRelease)
	assert.EqualValues(t, "57db51c", v.Build)

	assert.EqualValues(t, proto.CompactTextString(v), v.String())
}
