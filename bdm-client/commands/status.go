package commands

import (
	"fmt"
	_ "github.com/bmizerany/pq"
	"github.com/sburnett/bismark-tools/bdm-client/datastore"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

type status struct{}

func NewStatus() BdmCommand {
	return new(status)
}

func (status) Name() string {
	return "status"
}

func (status) Description() string {
	return "Show information about a single device"
}

func (status) Run(args []string) error {
	db, err := datastore.NewPostgresDatastore()
	if err != nil {
		return err
	}
	defer db.Close()

	if len(args) == 0 {
		return summarizeStatus(db)
	}

	var result *datastore.DevicesResult
	var nodeIds []string
	rowCount := 0
	for r := range db.SelectDevices([]datastore.Identifier{datastore.NodeId}, []datastore.Order{datastore.Ascending}, 0, args[0], "", "", nil) {
		rowCount++
		if r.Error != nil {
			return r.Error
		}
		result = r
		nodeIds = append(nodeIds, r.NodeId)
	}
	if rowCount == 0 {
		fmt.Fprintf(os.Stderr, "Device %s doesn't exist\n", args[0])
		os.Exit(1)
	} else if rowCount > 1 {
		fmt.Fprintln(os.Stderr, "That device ID is ambiguous:", strings.Join(nodeIds, ", "))
		os.Exit(1)
	}

	switch result.DeviceStatus {
	case datastore.Online:
		fmt.Printf("%s is online and should be sending its next probe in about %s.\n", result.NodeId, result.NextProbe)
		fmt.Printf("Its public IP address is %s and its firmware version is %s.\n", result.IpAddress, result.Version)
	case datastore.Stale:
		fmt.Printf("%s is about %s late sending its next probe.\n", result.NodeId, time.Duration(-1)*result.NextProbe)
		fmt.Printf("Its public IP address is %s and its firmware version is %s.\n", result.IpAddress, result.Version)
	case datastore.Offline:
		fmt.Printf("%s has been offline for %s, since %s.\n", result.NodeId, result.OutageDurationText, result.LastSeen)
		fmt.Printf("Its last known public IP address was %s and its firmware version was %s.\n", result.IpAddress, result.Version)
	default:
		panic(fmt.Errorf("Unknown device status"))
	}

	return nil
}

func summarizeStatus(db datastore.Datastore) error {
	var total, online, stale, offline, offlineHour, offlineDay, offlineWeek, offlineMonth int
	for r := range db.SelectDevices([]datastore.Identifier{datastore.NodeId}, []datastore.Order{datastore.Ascending}, 0, "", "", "", nil) {
		if r.Error != nil {
			return r.Error
		}

		total++
		switch r.DeviceStatus {
		case datastore.Online:
			online++
		case datastore.Stale:
			stale++
		case datastore.Offline:
			offline++
		}
		if r.DeviceStatus != datastore.Offline {
			continue
		}
		switch {
		case r.OutageDuration <= time.Hour:
			offlineHour++
		case r.OutageDuration <= time.Duration(24)*time.Hour:
			offlineDay++
		case r.OutageDuration <= time.Duration(24*30)*time.Hour:
			offlineMonth++
		}
	}

	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer writer.Flush()
	fprintWithTabs(writer, "DEVICE STATUS", "COUNT", "PERCENTAGE")
	fprintWithTabs(writer, "Online", online, percentage(online, total))
	fprintWithTabs(writer, "Stale", stale, percentage(stale, total))
	fprintWithTabs(writer, "Offline", offline, percentage(offline, total))
	fprintWithTabs(writer, "  past hour", offline-offlineHour, percentage(offline-offlineHour, total))
	fprintWithTabs(writer, "  past day", offline-offlineDay, percentage(offline-offlineDay, total))
	fprintWithTabs(writer, "  past week", offline-offlineWeek, percentage(offline-offlineWeek, total))
	fprintWithTabs(writer, "  past month", offline-offlineMonth, percentage(offline-offlineMonth, total))
	fprintWithTabs(writer, "Total", total, "100%")

	return nil
}
