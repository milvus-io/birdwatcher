package kv

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestTiKVLoad(te *testing.T) {
	te.Run("kv SaveAndLoad", func(t *testing.T) {
		for _, kv := range kvClients {
			ctx := context.TODO()
			err := kv.RemoveWithPrefix(ctx, "")
			require.NoError(t, err)

			defer kv.RemoveWithPrefix(ctx, "")

			saveAndLoadTests := []struct {
				key   string
				value string
			}{
				{"test1", "value1"},
				{"test2", "value2"},
				{"test1/a", "value_a"},
				{"test1/b", "value_b"},
			}

			for i, test := range saveAndLoadTests {
				if i < 4 {
					err = kv.Save(ctx, test.key, test.value)
					assert.NoError(t, err)
				}

				val, err := kv.Load(ctx, test.key)
				assert.NoError(t, err)
				assert.Equal(t, test.value, val)
			}

			invalidLoadTests := []struct {
				invalidKey string
			}{
				{"t"},
				{"a"},
				{"test1a"},
			}

			for _, test := range invalidLoadTests {
				val, err := kv.Load(ctx, test.invalidKey)
				assert.Error(t, err)
				assert.Zero(t, val)
			}

			loadPrefixTests := []struct {
				prefix string

				expectedKeys   []string
				expectedValues []string
				expectedError  error
			}{
				{"test", []string{
					"test1",
					"test2",
					"test1/a",
					"test1/b",
				}, []string{"value1", "value2", "value_a", "value_b"}, nil},
				{"test1", []string{
					"test1",
					"test1/a",
					"test1/b",
				}, []string{"value1", "value_a", "value_b"}, nil},
				{"test2", []string{"test2"}, []string{"value2"}, nil},
				{"", []string{
					"test1",
					"test2",
					"test1/a",
					"test1/b",
				}, []string{"value1", "value2", "value_a", "value_b"}, nil},
				{"test1/a", []string{"test1/a"}, []string{"value_a"}, nil},
				{"a", []string{}, []string{}, nil},
				{"root", []string{}, []string{}, nil},
				{"/tikv/test/root", []string{}, []string{}, nil},
			}

			for _, test := range loadPrefixTests {
				actualKeys, actualValues, err := kv.LoadWithPrefix(ctx, test.prefix)
				assert.ElementsMatch(t, test.expectedKeys, actualKeys)
				assert.ElementsMatch(t, test.expectedValues, actualValues)
				assert.Equal(t, test.expectedError, err)
			}

			removeTests := []struct {
				validKey   string
				invalidKey string
			}{
				{"test1", "abc"},
				{"test1/a", "test1/lskfjal"},
				{"test1/b", "test1/b"},
				{"test2", "-"},
			}

			for _, test := range removeTests {
				err = kv.Remove(ctx, test.validKey)
				assert.NoError(t, err)

				_, err = kv.Load(ctx, test.validKey)
				assert.Error(t, err)

				err = kv.Remove(ctx, test.validKey)
				assert.NoError(t, err)
				err = kv.Remove(ctx, test.invalidKey)
				assert.NoError(t, err)
			}

			removeWithPrefixTests := []struct {
				key   string
				value string
			}{
				{"testr1", "value1"},
				{"testr2", "value2"},
				{"testr1/a", "value_a"},
				{"testr1/b", "value_b"},
				{"testr2/c", "value3"},
			}
			for _, test := range removeWithPrefixTests {
				err = kv.Save(ctx, test.key, test.value)
				assert.NoError(t, err)
			}

			err = kv.RemoveWithPrefix(ctx, "testr1")
			assert.NoError(t, err)
			keys, vals, err := kv.LoadWithPrefix(ctx, "testr")
			assert.NoError(t, err)
			assert.ElementsMatch(t, []string{"testr2", "testr2/c"}, keys)
			assert.ElementsMatch(t, []string{"value2", "value3"}, vals)
			// allow remove with non-exist prefix
			err = kv.RemoveWithPrefix(ctx, "testnoexist")
			assert.NoError(t, err)
		}
	})
}

func TestGetAllRootPath(t *testing.T) {
	for _, kv := range kvClients {
		ctx := context.TODO()
		defer kv.RemoveWithPrefix(ctx, "")

		tests := []struct {
			key   string
			value string
		}{
			{"testr1", "value1"},
			{"testr2", "value2"},
			{"testr1/a", "value_a"},
			{"testr1/a/a2", "value_a"},
			{"testr1/b", "value_b"},
			{"testr2/c", "value3"},
			{"testr3", "value3"},
		}
		for _, test := range tests {
			err := kv.Save(ctx, test.key, test.value)
			assert.NoError(t, err)
		}
		roots, err := kv.GetAllRootPath(ctx)
		assert.NoError(t, err)
		assert.Equal(t, len(roots), 3)
		assert.ElementsMatch(t, []string{"testr1", "testr2", "testr3"}, roots)
	}
}

func TestRemoveWithPrev(t *testing.T) {
	for _, kv := range kvClients {
		ctx := context.TODO()
		defer kv.RemoveWithPrefix(ctx, "")

		tests := []struct {
			key   string
			value string
		}{
			{"testr1", "value1"},
			{"testr2", "value2"},
			{"testr1/a", "value_a"},
			{"testr1/b", "value_b"},
			{"testr2/c", "value3"},
		}
		for _, test := range tests {
			err := kv.Save(ctx, test.key, test.value)
			assert.NoError(t, err)
		}
		kvs, err := kv.removeWithPrefixAndPrevKV(ctx, "testr1")
		assert.NoError(t, err)
		assert.Equal(t, len(kvs), 3)
		var pks, pvs []string
		for _, kv := range kvs {
			pks = append(pks, string(kv.Key))
			pvs = append(pvs, string(kv.Value))
		}
		assert.ElementsMatch(t, []string{"testr1", "testr1/a", "testr1/b"}, pks)
		assert.ElementsMatch(t, []string{"value1", "value_a", "value_b"}, pvs)
		keys, vals, err := kv.LoadWithPrefix(ctx, "testr")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"testr2", "testr2/c"}, keys)
		assert.ElementsMatch(t, []string{"value2", "value3"}, vals)
	}

	for _, kv := range kvClients {
		ctx := context.TODO()
		defer kv.RemoveWithPrefix(ctx, "")

		tests := []struct {
			key   string
			value string
		}{
			{"testr1", "value1"},
			{"testr2", "value2"},
			{"testr1/a", "value_a"},
			{"testr1/b", "value_b"},
			{"testr2/c", "value3"},
		}
		for _, test := range tests {
			err := kv.Save(ctx, test.key, test.value)
			assert.NoError(t, err)
		}
		// it's fine if the key doesn't exist.
		kvp, err := kv.removeWithPrevKV(ctx, "not_exist_key")
		assert.NoError(t, err)
		assert.Equal(t, kvp, (*mvccpb.KeyValue)(nil))
		kvp, err = kv.removeWithPrevKV(ctx, "testr1")
		assert.NoError(t, err)
		assert.Equal(t, string(kvp.Key), "testr1")
		assert.Equal(t, string(kvp.Value), "value1")
		keys, vals, err := kv.LoadWithPrefix(ctx, "testr")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"testr2", "testr1/a", "testr1/b", "testr2/c"}, keys)
		assert.ElementsMatch(t, []string{"value2", "value_a", "value_b", "value3"}, vals)
		// it's fine if the key doesn't exist.
		kvp, err = kv.removeWithPrevKV(ctx, "testr1")
		assert.NoError(t, err)
		assert.Equal(t, kvp, (*mvccpb.KeyValue)(nil))
	}
}

func TestEmptyKey(t *testing.T) {
	for _, kv := range kvClients {
		ctx := context.TODO()
		err := kv.RemoveWithPrefix(ctx, "")
		require.NoError(t, err)

		defer kv.RemoveWithPrefix(ctx, "")

		err = kv.Save(ctx, "key", "")
		assert.NoError(t, err)

		val, err := kv.Load(ctx, "key")
		assert.NoError(t, err)
		assert.Equal(t, val, "")

		_, vals, err := kv.LoadWithPrefix(ctx, "key")
		assert.NoError(t, err)
		assert.Equal(t, vals[0], "")
	}
}

func TestScanSize(t *testing.T) {
	for _, kv := range kvClients {
		ctx := context.TODO()
		scanSize := SnapshotScanSize
		err := kv.RemoveWithPrefix(ctx, "")
		require.NoError(t, err)

		defer kv.RemoveWithPrefix(ctx, "")

		// Test total > scansize
		// key_map := map[string]string{}
		for i := 1; i <= scanSize+100; i++ {
			a := fmt.Sprintf("%v", i)
			// key_map[a] = a
			err := kv.Save(ctx, a, a)
			assert.NoError(t, err)
		}

		keys, _, err := kv.LoadWithPrefix(ctx, "")
		assert.NoError(t, err)
		assert.Equal(t, len(keys), scanSize+100)

		err = kv.RemoveWithPrefix(ctx, "")
		require.NoError(t, err)
	}
}

func TestBackupKV(t *testing.T) {
	for i, kv := range kvClients {
		ctx := context.TODO()
		defer kv.RemoveWithPrefix(ctx, "")

		tests := []struct {
			key   string
			value string
		}{
			{"r1/testr1", "value1"},
			{"r1/testr2", "value2"},
			{"r1/testr1/a", "value3"},
			{"r1/testr1/a/a2", "value4"},
			{"r1/testr1/b", "value5"},
			{"r1/testr2/c", "value6"},
			{"r1/testr3", "value7"},
			{"r1/testr4", "value7"},
			{"r2/testr1/a/a2", "value8"},
			{"r2/testr1/a/a3", "value9"},
		}
		for _, test := range tests {
			err := kv.Save(ctx, test.key, test.value)
			assert.NoError(t, err)
		}
		now := time.Now()
		filePath := fmt.Sprintf("/tmp/bw_test_%d.%s.bak.gz", i, now.Format("060102-150405"))
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		assert.NoError(t, err)
		defer f.Close()

		gw := gzip.NewWriter(f)
		defer gw.Close()
		w := bufio.NewWriter(gw)
		err = kv.BackupKV("r1", "testr1", w, false, 100)
		assert.NoError(t, err)
	}
}
