package ifc

// SubscriptionInitialPosition is the type of a subscription initial position
type SubscriptionInitialPosition int

const (
	// SubscriptionPositionUnkown indicates we don't care about the consumer location, since we are doing another seek or only some meta api over that
	SubscriptionPositionUnknown SubscriptionInitialPosition = iota

	// SubscriptionPositionLatest is latest position which means the start consuming position will be the last message
	SubscriptionPositionLatest

	// SubscriptionPositionEarliest is earliest position which means the start consuming position will be the first message
	SubscriptionPositionEarliest
)

type MqOption struct {
	SubscriptionInitPos SubscriptionInitialPosition
}
