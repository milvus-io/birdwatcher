package autocomplete

import (
	"sort"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

// makeCmd creates a cobra command with flags for testing.
func makeCmd(use string, flags func(*pflag.FlagSet)) *cobra.Command {
	cmd := &cobra.Command{Use: use}
	if flags != nil {
		flags(cmd.Flags())
	}
	return cmd
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func TestStaticValueSuggestions(t *testing.T) {
	cmd := makeCmd("mycmd", func(fs *pflag.FlagSet) {
		fs.String("format", "", "output format")
		_ = fs.SetAnnotation("format", "values", []string{"default", "json", "table", "line", "plain"})
		fs.Int64("collection", 0, "collection id")
	})
	commands := []*cobra.Command{cmd}

	t.Run("all values on empty input after flag", func(t *testing.T) {
		result := SuggestInputCommands("mycmd --format ", commands)
		keys := sortedKeys(result)
		assert.Equal(t, []string{"default", "json", "line", "plain", "table"}, keys)
	})

	t.Run("partial match filters values", func(t *testing.T) {
		result := SuggestInputCommands("mycmd --format j", commands)
		keys := sortedKeys(result)
		assert.Equal(t, []string{"json"}, keys)
	})

	t.Run("partial match with equals sign", func(t *testing.T) {
		result := SuggestInputCommands("mycmd --format=j", commands)
		keys := sortedKeys(result)
		assert.Equal(t, []string{"json"}, keys)
	})

	t.Run("consumed value returns to flags", func(t *testing.T) {
		// After value is consumed, typing -- should suggest flags again
		result := SuggestInputCommands("mycmd --format json --", commands)
		assert.Contains(t, result, "--format")
		assert.Contains(t, result, "--collection")
		assert.NotContains(t, result, "json")
	})

	t.Run("flag without values gives no value suggestions", func(t *testing.T) {
		result := SuggestInputCommands("mycmd --collection ", commands)
		// collection has no values annotation, so no value suggestions
		// Should return empty since there are no matching candidates for bare input
		assert.Empty(t, result)
	})

	t.Run("multiple prefix match", func(t *testing.T) {
		result := SuggestInputCommands("mycmd --format d", commands)
		keys := sortedKeys(result)
		assert.Equal(t, []string{"default"}, keys)
	})

	t.Run("no match returns empty", func(t *testing.T) {
		result := SuggestInputCommands("mycmd --format xyz", commands)
		assert.Empty(t, result)
	})

	t.Run("partial p matches plain", func(t *testing.T) {
		result := SuggestInputCommands("mycmd --format p", commands)
		keys := sortedKeys(result)
		assert.Equal(t, []string{"plain"}, keys)
	})
}

func TestNamedValueSuggester(t *testing.T) {
	RegisterValueSuggester("test-colors", ValueSuggestFunc(func(partial string) []string {
		all := []string{"red", "green", "blue"}
		var result []string
		for _, v := range all {
			if len(partial) == 0 || v[:1] == partial[:1] {
				result = append(result, v)
			}
		}
		return result
	}))
	defer UnregisterValueSuggester("test-colors")

	cmd := makeCmd("paint", func(fs *pflag.FlagSet) {
		fs.String("color", "", "color to use")
		_ = fs.SetAnnotation("color", "valuesSuggester", []string{"test-colors"})
	})
	commands := []*cobra.Command{cmd}

	t.Run("named suggester returns values", func(t *testing.T) {
		result := SuggestInputCommands("paint --color ", commands)
		assert.NotEmpty(t, result)
	})

	t.Run("named suggester with partial", func(t *testing.T) {
		result := SuggestInputCommands("paint --color r", commands)
		assert.Contains(t, result, "red")
	})
}

func TestValueSuggesterRegistry(t *testing.T) {
	t.Run("register and get", func(t *testing.T) {
		fn := ValueSuggestFunc(func(partial string) []string { return []string{"a"} })
		RegisterValueSuggester("test-reg", fn)
		defer UnregisterValueSuggester("test-reg")

		s, ok := GetValueSuggester("test-reg")
		assert.True(t, ok)
		assert.Equal(t, []string{"a"}, s.Suggest(""))
	})

	t.Run("unregistered returns false", func(t *testing.T) {
		_, ok := GetValueSuggester("nonexistent")
		assert.False(t, ok)
	})

	t.Run("unregister removes", func(t *testing.T) {
		RegisterValueSuggester("test-unreg", ValueSuggestFunc(func(string) []string { return nil }))
		UnregisterValueSuggester("test-unreg")
		_, ok := GetValueSuggester("test-unreg")
		assert.False(t, ok)
	})
}

func TestSubcommandWithValues(t *testing.T) {
	showCmd := &cobra.Command{Use: "show"}
	collectionCmd := makeCmd("collection", func(fs *pflag.FlagSet) {
		fs.String("format", "", "output format")
		_ = fs.SetAnnotation("format", "values", []string{"default", "json", "table"})
		fs.Int64("id", 0, "collection id")
	})
	showCmd.AddCommand(collectionCmd)
	commands := []*cobra.Command{showCmd}

	t.Run("nested command value suggestion", func(t *testing.T) {
		result := SuggestInputCommands("show collection --format ", commands)
		keys := sortedKeys(result)
		assert.Equal(t, []string{"default", "json", "table"}, keys)
	})

	t.Run("nested command partial value", func(t *testing.T) {
		result := SuggestInputCommands("show collection --format j", commands)
		assert.Equal(t, map[string]string{"json": ""}, result)
	})

	t.Run("nested command value consumed", func(t *testing.T) {
		// After value is consumed, typing -- should suggest flags again
		result := SuggestInputCommands("show collection --format json --", commands)
		assert.Contains(t, result, "--format")
		assert.Contains(t, result, "--id")
	})
}
