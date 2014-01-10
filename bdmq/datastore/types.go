package datastore

import (
	"fmt"
)

type Identifier int

const (
	NodeId Identifier = iota
	IpAddress
	Version
	LastProbe
	OutageDuration
)

func (ident Identifier) String() string {
	switch ident {
	case NodeId:
		return "id"
	case IpAddress:
		return "ip"
	case Version:
		return "bversion"
	case LastProbe:
		return "last_probe"
	case OutageDuration:
		return "outage_duration"
	default:
		panic(fmt.Errorf("Missing Identifier.String() case"))
	}
}

type Order int

const (
	Ascending Order = iota
	Descending
)

func (order Order) String() string {
	switch order {
	case Ascending:
		return "ASC"
	case Descending:
		return "DESC"
	default:
		panic(fmt.Errorf("Missing Order.String() case"))
	}
}

type DeviceStatus int

const (
	Online DeviceStatus = iota
	Stale
	Offline
)

func (status DeviceStatus) String() string {
	switch status {
	case Online:
		return "up"
	case Stale:
		return "stale"
	case Offline:
		return "down"
	default:
		panic(fmt.Errorf("Missing Order.DeviceStatus() case"))
	}
}

func OutageDurationToDeviceStatus(outageDurationSeconds float64) DeviceStatus {
	switch {
	case outageDurationSeconds <= 90:
		return Online
	case outageDurationSeconds <= 600:
		return Stale
	default:
		return Offline
	}
}
