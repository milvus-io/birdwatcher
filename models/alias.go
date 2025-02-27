package models

import "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"

type Alias struct {
	Name         string
	CollectionID int64
	State        AliasState
	CreateTS     uint64
	DBID         int64

	key string
}

func NewAlias(info *etcdpb.AliasInfo, key string) *Alias {
	a := &Alias{
		Name:         info.GetAliasName(),
		CollectionID: info.GetCollectionId(),
		State:        AliasState(info.GetState()),
		CreateTS:     info.GetCreatedTime(),
		DBID:         info.GetDbId(),
	}
	return a
}

type AliasState int32

const (
	AliasStateAliasCreated  AliasState = 0
	AliasStateAliasCreating AliasState = 1
	AliasStateAliasDropping AliasState = 2
	AliasStateAliasDropped  AliasState = 3
)

var AliasStateName = map[int32]string{
	0: "AliasCreated",
	1: "AliasCreating",
	2: "AliasDropping",
	3: "AliasDropped",
}

var AliasStateValue = map[string]int32{
	"AliasCreated":  0,
	"AliasCreating": 1,
	"AliasDropping": 2,
	"AliasDropped":  3,
}

func (x AliasState) String() string {
	return EnumName(AliasStateName, int32(x))
}
