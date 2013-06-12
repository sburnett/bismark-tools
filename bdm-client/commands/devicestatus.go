package commands

import (
	"fmt"
)

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
	}
	panic(fmt.Errorf("Invalid status"))
}

func ParseDeviceStatus(text string) (DeviceStatus, error) {
	switch text {
	case "up", "online":
		return Online, nil
	case "stale", "late":
		return Stale, nil
	case "down", "offline":
		return Offline, nil
	default:
		return Offline, fmt.Errorf("Invalid status")
	}
}

func OutageDurationToDeviceStatus(outageDurationSeconds float64) DeviceStatus {
	switch {
	case outageDurationSeconds <= 60:
		return Online
	case outageDurationSeconds <= 600:
		return Stale
	default:
		return Offline
	}
}

func OutageDurationToNextProbe(outageDurationSeconds float64) float64 {
	return 60 - outageDurationSeconds
}
