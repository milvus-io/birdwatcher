package models

type Channel struct {
	PhysicalName  string
	VirtualName   string
	StartPosition *MsgPosition
}
