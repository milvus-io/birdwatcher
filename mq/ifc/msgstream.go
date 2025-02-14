package ifc

import (
	"encoding/binary"
)

type Consumer interface {
	GetLastMessageID() (MessageID, error)
	GetLastMessage() (Message, error)
	Consume() (Message, error)
	Seek(MessageID) error
	Close() error
}

var (
	Endian              = binary.LittleEndian
	DefaultPartitionIdx = int32(0)
)
