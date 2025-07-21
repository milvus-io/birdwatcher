package healthz

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
)

var (
	allCheckItems map[string]HealthzCheckItem
	defaultList   []string
)

func init() {
	allCheckItems = make(map[string]HealthzCheckItem)

	allCheckItems["ISS43407"] = newIss43407()
	allCheckItems["QUERYVIEW_LAG"] = newQueryViewLag()

	defaultList = []string{
		"ISS43407",
	}
}

func AllCheckItems() []HealthzCheckItem {
	return lo.Values(allCheckItems)
}

func DefaultCheckItems() []HealthzCheckItem {
	var items []HealthzCheckItem
	for _, name := range defaultList {
		items = append(items, allCheckItems[name])
	}
	return items
}

func GetHealthzCheckItem(name string) (HealthzCheckItem, bool) {
	item, ok := allCheckItems[name]
	return item, ok
}

type HealthzCheckReport struct {
	Msg   string
	Extra map[string]any
}

type HealthzCheckReports struct {
	framework.ListResultSet[*HealthzCheckReport]
}

func (rs *HealthzCheckReports) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, report := range rs.Data {
			fmt.Fprintln(sb, report.Msg)
		}
		return sb.String()
	case framework.FormatJSON:
		sb := &strings.Builder{}
		for _, report := range rs.Data {
			output := report.Extra
			bs, err := json.Marshal(output)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			sb.Write(bs)
			sb.WriteString("\n")
		}
		return sb.String()
	default:
	}
	return ""
}

// HealthzCheckItem defines a known healthz check item.
type HealthzCheckItem interface {
	Name() string
	Description() string
	Check(ctx context.Context, client metakv.MetaKV, basePath string) ([]*HealthzCheckReport, error)
}

type checkItemBase struct {
	name        string
	description string
}

func (c checkItemBase) Name() string {
	return c.name
}

func (c checkItemBase) Description() string {
	return c.description
}
