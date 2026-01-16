package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type ConfigEtcdParam struct {
	framework.ParamBase `use:"show config-etcd" desc:"list configuations set by etcd source"`
	Format              string `name:"format" default:"" desc:"output format (default, json)"`
}

// ConfigEtcdCommand return show config-etcd command.
func (c *ComponentShow) ConfigEtcdCommand(ctx context.Context, p *ConfigEtcdParam) (*framework.PresetResultSet, error) {
	keys, values, err := common.ListEtcdConfigs(ctx, c.client, c.basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to list configurations from etcd: %w", err)
	}

	configs := make([]ConfigItem, 0, len(keys))
	for i, key := range keys {
		configs = append(configs, ConfigItem{
			Key:   key,
			Value: values[i],
		})
	}

	rs := &ConfigEtcdResult{configs: configs}
	return framework.NewPresetResultSet(rs, framework.NameFormat(p.Format)), nil
}

type ConfigItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type ConfigEtcdResult struct {
	configs []ConfigItem
}

func (rs *ConfigEtcdResult) Entities() any {
	return rs.configs
}

func (rs *ConfigEtcdResult) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		return rs.printDefault()
	case framework.FormatJSON:
		return rs.printAsJSON()
	}
	return ""
}

func (rs *ConfigEtcdResult) printDefault() string {
	sb := &strings.Builder{}
	for _, config := range rs.configs {
		fmt.Fprintf(sb, "Key: %s, Value: %s\n", config.Key, config.Value)
	}
	return sb.String()
}

func (rs *ConfigEtcdResult) printAsJSON() string {
	type OutputJSON struct {
		Configs []ConfigItem `json:"configs"`
		Total   int          `json:"total"`
	}

	output := OutputJSON{
		Configs: rs.configs,
		Total:   len(rs.configs),
	}

	return framework.MarshalJSON(output)
}
