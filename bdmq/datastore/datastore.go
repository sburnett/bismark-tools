package datastore

import (
	"time"
)

type DevicesResult struct {
	NodeId, IpAddress, Version string
	LastSeen                   time.Time
	DeviceStatus               DeviceStatus
	OutageDuration             time.Duration
	NextProbe                  time.Duration
	OutageDurationText         string

	Error error
}

type VersionsResult struct {
	Version            string
	Count, OnlineCount int

	Error error
}

type Datastore interface {
	SelectDevices(orderBy []Identifier, order []Order, limit int, nodeIdConstraint, ipAddressConstraint, versionConstraint string, deviceStatusConstraint *DeviceStatus) chan *DevicesResult
	SelectVersions() chan *VersionsResult
	Close()
}
