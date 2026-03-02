package healthz

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
)

type GrantAliasCheck struct {
	checkItemBase
}

func newGrantAliasCheck() *GrantAliasCheck {
	return &GrantAliasCheck{
		checkItemBase: checkItemBase{
			name:        "GRANT_ALIAS_CHECK",
			description: `Check whether any RBAC grants are using alias names instead of real collection names`,
		},
	}
}

func (i *GrantAliasCheck) Check(ctx context.Context, client metakv.MetaKV, basePath string) ([]*HealthzCheckReport, error) {
	// 1. Load all aliases
	aliases, err := common.ListAlias(ctx, client, basePath, "", func(a *models.Alias) bool {
		return a.State == models.AliasStateAliasCreated
	})
	if err != nil {
		return nil, err
	}

	aliasNames := make(map[string]int64) // aliasName -> collectionID
	for _, a := range aliases {
		aliasNames[a.Name] = a.CollectionID
	}

	if len(aliasNames) == 0 {
		return nil, nil
	}

	// 2. Load all grants
	grants, err := common.ListGrantEntries(ctx, client, basePath)
	if err != nil {
		return nil, err
	}

	// 3. Cross-check: find grants whose objectName is an alias
	var results []*HealthzCheckReport
	for _, g := range grants {
		if g.ObjectType != "Collection" {
			continue
		}
		if g.ObjectName == "*" {
			continue
		}
		if collID, ok := aliasNames[g.ObjectName]; ok {
			results = append(results, &HealthzCheckReport{
				Item: i.Name(),
				Msg:  fmt.Sprintf("Grant for role %q on alias %q (db: %s) points to collectionID %d", g.Role, g.ObjectName, g.DBName, collID),
				Extra: map[string]any{
					"role":          g.Role,
					"db_name":       g.DBName,
					"alias_name":    g.ObjectName,
					"collection_id": collID,
				},
			})
		}
	}

	return results, nil
}
