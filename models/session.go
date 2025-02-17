package models

import (
	"fmt"
	"net"
)

// Session is the json model for milvus session struct in etcd.
type Session struct {
	ServerID   int64  `json:"ServerID,omitempty"`
	ServerName string `json:"ServerName,omitempty"`
	Address    string `json:"Address,omitempty"`
	Exclusive  bool   `json:"Exclusive,omitempty"`
	Version    string `json:"Version,omitempty"`

	key string
}

func (s *Session) SetKey(key string) {
	s.key = key
}

func (s *Session) GetKey() string {
	return s.key
}

func (s Session) String() string {
	return fmt.Sprintf("Session:%s, ServerID: %d, Version: %s, Address: %s", s.ServerName, s.ServerID, s.Version, s.Address)
}

func (s *Session) IP() string {
	addr, err := net.ResolveTCPAddr("tcp", s.Address)
	if err != nil {
		return ""
	}
	return addr.IP.To4().String()
}
