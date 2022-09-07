package models

import "fmt"

// Session is the json model for milvus session struct in etcd.
type Session struct {
	ServerID   int64  `json:"ServerID,omitempty"`
	ServerName string `json:"ServerName,omitempty"`
	Address    string `json:"Address,omitempty"`
	Exclusive  bool   `json:"Exclusive,omitempty"`
}

func (s Session) String() string {
	return fmt.Sprintf("Session:%s, ServerID: %d", s.ServerName, s.ServerID)
}
