package commands

import (
	"fmt"
	_ "github.com/bmizerany/pq"
	"github.com/sburnett/bismark-tools/bdm-client/datastore"
	"os"
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
	return "Show whether a device is online"
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

	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer writer.Flush()
	rowCount := 0
	for r := range db.SelectDevices(datastore.NodeId, datastore.Ascending, 0, args[0], "", "", nil) {
		rowCount++
		fprintWithTabs(writer, r.NodeId, r.DeviceStatus)
	}

	if rowCount == 0 {
		fmt.Fprintln(os.Stderr, "That device doesn't exist")
		os.Exit(1)
	}

	return nil
}

func summarizeStatus(db datastore.Datastore) error {
	var total, online, stale, offline, offlineHour, offlineDay, offlineWeek, offlineMonth int
	for r := range db.SelectDevices(datastore.NodeId, datastore.Ascending, 0, "", "", "", nil) {
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
