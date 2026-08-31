package milvusctl

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/autocomplete"
	"github.com/milvus-io/milvus/client/v3/milvusclient"
)

const (
	milvusCollectionSuggester = "milvus-collection"
	milvusFieldSuggester      = "milvus-field"
	milvusIndexSuggester      = "milvus-index"
	milvusSuggestionTimeout   = 2 * time.Second
)

type ConnectMilvusParam struct {
	framework.ParamBase `use:"connect milvus" desc:"connect to a Milvus instance"`
	Address             string `name:"address" default:"localhost:19530" desc:"Milvus server address (host:port)"`
	Username            string `name:"username" default:"" desc:"auth username"`
	Password            string `name:"password" default:"" desc:"auth password"`
	APIKey              string `name:"api-key" default:"" desc:"API key (overrides user/pass)"`
	DB                  string `name:"db" default:"" desc:"database name"`
	TLS                 bool   `name:"tls" default:"false" desc:"enable TLS"`
	Timeout             string `name:"timeout" default:"10s" desc:"dial timeout, e.g. 10s / 500ms; 0 disables"`
}

func ConnectMilusctl(ctx context.Context, p *ConnectMilvusParam, parent *framework.CmdState) (*MilvusctlState, error) {
	cfg := &milvusclient.ClientConfig{
		Address:       p.Address,
		Username:      p.Username,
		Password:      p.Password,
		APIKey:        p.APIKey,
		DBName:        p.DB,
		EnableTLSAuth: p.TLS,
	}
	cli, err := milvusclient.New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &MilvusctlState{
		CmdState:  parent.Spawn(fmt.Sprintf("Milvusctl[%s]", cfg.Address)),
		clientcfg: cfg,
		client:    cli,
	}, nil
}

type MilvusctlState struct {
	*framework.CmdState
	clientcfg *milvusclient.ClientConfig
	client    *milvusclient.Client
}

// Label overrides default cmd label behavior
// returning OSS[PROVIDER](BUCKET/CURR_DIR)
func (s *MilvusctlState) Label() string {
	return fmt.Sprintf("Milvusctl[%s]", s.clientcfg.Address)
}

func (s *MilvusctlState) Close() {
	autocomplete.UnregisterValueSuggester(milvusCollectionSuggester)
	autocomplete.UnregisterValueSuggester(milvusFieldSuggester)
	autocomplete.UnregisterValueSuggester(milvusIndexSuggester)
	if s.client != nil {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_ = s.client.Close(ctx)
	}
}

func (s *MilvusctlState) SetupCommands() {
	cmd := s.GetCmd()

	s.MergeFunctionCommands(cmd, s)
	s.UpdateState(cmd, s, s.SetupCommands)

	autocomplete.RegisterValueSuggester(milvusCollectionSuggester,
		autocomplete.ContextValueSuggestFunc(s.suggestCollections))
	autocomplete.RegisterValueSuggester(milvusFieldSuggester,
		autocomplete.ContextValueSuggestFunc(s.suggestFields))
	autocomplete.RegisterValueSuggester(milvusIndexSuggester,
		autocomplete.ContextValueSuggestFunc(s.suggestIndexes))
}

func (s *MilvusctlState) suggestCollections(partial string, _ autocomplete.ValueSuggestionContext) []string {
	ctx, cancel := context.WithTimeout(context.Background(), milvusSuggestionTimeout)
	defer cancel()
	names, err := s.client.ListCollections(ctx, milvusclient.NewListCollectionOption())
	if err != nil {
		return nil
	}
	return filterMilvusSuggestions(names, partial)
}

func (s *MilvusctlState) suggestFields(partial string, suggestCtx autocomplete.ValueSuggestionContext) []string {
	collection := strings.TrimSpace(suggestCtx.FlagValues["collection"])
	if collection == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), milvusSuggestionTimeout)
	defer cancel()
	coll, err := s.client.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collection))
	if err != nil || coll.Schema == nil {
		return nil
	}
	names := make([]string, 0, len(coll.Schema.Fields))
	for _, field := range coll.Schema.Fields {
		if field != nil {
			names = append(names, field.Name)
		}
	}
	return filterMilvusSuggestions(names, partial)
}

func (s *MilvusctlState) suggestIndexes(partial string, suggestCtx autocomplete.ValueSuggestionContext) []string {
	collection := strings.TrimSpace(suggestCtx.FlagValues["collection"])
	if collection == "" {
		return nil
	}

	option := milvusclient.NewListIndexOption(collection)
	if field := strings.TrimSpace(suggestCtx.FlagValues["field"]); field != "" {
		option = option.WithFieldName(field)
	}
	ctx, cancel := context.WithTimeout(context.Background(), milvusSuggestionTimeout)
	defer cancel()
	names, err := s.client.ListIndexes(ctx, option)
	if err != nil {
		return nil
	}
	return filterMilvusSuggestions(names, partial)
}

func filterMilvusSuggestions(values []string, partial string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if !strings.HasPrefix(value, partial) {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
