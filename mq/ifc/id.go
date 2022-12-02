package ifc

// MessageID is the ifc that provides operations of message is
type MessageID interface {
	Serialize() []byte

	AtEarliestPosition() bool

	LessOrEqualThan(msgID []byte) (bool, error)

	Equal(msgID []byte) (bool, error)

	String() string
}
