package ifc

type Message interface {
	Topic() string

	Payload() []byte

	ID() MessageID
}
