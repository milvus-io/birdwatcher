package models

import (
	etcdpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
)

// Database birdwatcher model for database entity.
type Database struct {
	ID          int64
	Name        string
	TenantID    string
	State       DatabaseState
	key         string
	CreatedTime uint64
}

type DatabaseState int32

const (
	DatabaseStateDatabaseUnknown  DatabaseState = 0
	DatabaseStateDatabaseCreated  DatabaseState = 1
	DatabaseStateDatabaseCreating DatabaseState = 2
	DatabaseStateDatabaseDropping DatabaseState = 3
	DatabaseStateDatabaseDropped  DatabaseState = 4
)

var DatabaseStatename = map[int32]string{
	0: "DatabaseUnknown",
	1: "DatabaseCreated",
	2: "DatabaseCreating",
	3: "DatabaseDropping",
	4: "DatabaseDropped",
}

var DatabaseStatevalue = map[string]int32{
	"DatabaseUnknown":  0,
	"DatabaseCreated":  1,
	"DatabaseCreating": 2,
	"DatabaseDropping": 3,
	"DatabaseDropped":  4,
}

func (x DatabaseState) String() string {
	return EnumName(DatabaseStatename, int32(x))
}

func NewDatabase(info *etcdpbv2.DatabaseInfo, key string) *Database {
	return &Database{
		ID:          info.GetId(),
		Name:        info.GetName(),
		TenantID:    info.GetTenantId(),
		State:       DatabaseState(info.GetState()),
		CreatedTime: info.GetCreatedTime(),
	}
}
