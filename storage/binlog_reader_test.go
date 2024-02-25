package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinlogReader(t *testing.T) {
	f, err := os.Open("/home/silverxia/Downloads/vdc628/434283793997103105/434283879691714565/434283881002434562")
	require.NoError(t, err)

	r, de, err := NewBinlogReader(f)
	require.NoError(t, err)
	fmt.Printf("%#v\n", de)
	ids, err := r.NextInt64EventReader()
	require.NoError(t, err)

	t.Log(len(ids))
}
