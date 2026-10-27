package manifest

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/require"
)

func TestReferenceValidatesSnapshotAndResolvesPaths(t *testing.T) {
	for _, raw := range []string{"", "null", `{}`, `{"ver":1}`, `{"base_path":"files/segment"}`, `{"base_path":"files/segment","ver":-1}`} {
		_, err := ParseReference(raw)
		require.Error(t, err, raw)
	}
	ref, err := ParseReference(`{"ver":7,"base_path":"files/segment"}`)
	require.NoError(t, err)
	require.Equal(t, "files/segment/_metadata/manifest-7.avro", ref.ObjectPath())
	for _, test := range []struct{ section, name, want string }{
		{"_data", "column.parquet", "files/segment/_data/column.parquet"},
		{"_delta", "../_delta/delete", "files/segment/_delta/delete"},
		{"_data", "s3://bucket/key", "s3://bucket/key"},
		{"_data", "ROOT_PATH/key", "ROOT_PATH/key"},
	} {
		got, err := ref.FilePath(test.section, test.name)
		require.NoError(t, err)
		require.Equal(t, test.want, got)
	}
	_, err = ref.FilePath("_data", " ")
	require.Error(t, err)
	ref, err = ParseReference(`{"ver":0,"base_path":"files/segment"}`)
	require.NoError(t, err)
	require.Zero(t, ref.Version)
}

func TestParseOCFCodecsAndTruncation(t *testing.T) {
	for _, codec := range []ocf.CodecName{ocf.Null, ocf.Deflate, ocf.Snappy} {
		t.Run(string(codec), func(t *testing.T) {
			var out bytes.Buffer
			e, err := ocf.NewEncoder(manifestTestSchema(VersionV4), &out, ocf.WithCodec(codec))
			require.NoError(t, err)
			want := &Manifest{ColumnGroups: []ColumnGroup{{Columns: []string{"100"}, Format: "parquet", Files: []ColumnGroupFile{{Path: "pk", StartIndex: 2, EndIndex: 5}}}}}
			require.NoError(t, e.Encode(want))
			require.NoError(t, e.Close())
			m, err := Parse(bytes.NewReader(out.Bytes()))
			require.NoError(t, err)
			require.Equal(t, want.ColumnGroups[0].Columns, m.ColumnGroups[0].Columns)
			require.EqualValues(t, 2, m.ColumnGroups[0].Files[0].StartIndex)
			for _, truncate := range []int{1, 8, 16, out.Len() - 5} {
				_, err := Parse(bytes.NewReader(out.Bytes()[:out.Len()-truncate]))
				require.Error(t, err, "truncated %d bytes", truncate)
			}
			bad := append([]byte(nil), out.Bytes()...)
			bad[len(bad)-1] ^= 1
			_, err = Parse(bytes.NewReader(bad))
			require.ErrorContains(t, err, "sync marker")
			_, err = Parse(bytes.NewReader(append(out.Bytes(), 0)))
			require.ErrorContains(t, err, "trailing data")
		})
	}
}
