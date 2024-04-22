package ifc

import (
	"encoding/binary"
)

type Consumer interface {
	GetLastMessageID() (MessageID, error)
	GetLastMessage() (Message, error)
	Consume() (Message, error)
	Close() error
}

var Endian = binary.LittleEndian
var DefaultPartitionIdx = int32(0)
