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

func (status) printSummaryTable() error {
	db, err := datastore.NewPostgresDatastore()
	if err != nil {
		return err
	}
	defer db.Close()

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
		if r.OutageDuration <= time.Hour {
			offlineHour++
		}
		if r.OutageDuration <= time.Duration(24)*time.Hour {
			offlineDay++
		}
		if r.OutageDuration <= time.Duration(24*7)*time.Hour {
			offlineWeek++
		}
		if r.OutageDuration <= time.Duration(24*30)*time.Hour {
			offlineMonth++
		}
	}

	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer writer.Flush()
	fprintWithTabs(writer, "DEVICE STATUS", "COUNT", "PERCENTAGE")
	fprintWithTabs(writer, "Online", online, percentage(online, total))
	fprintWithTabs(writer, "Stale", stale, percentage(stale, total))
	fprintWithTabs(writer, "Offline", offline, percentage(offline, total))
	fprintWithTabs(writer, "  past hour", offlineHour, percentage(offlineHour, total))
	fprintWithTabs(writer, "  past day", offlineDay, percentage(offlineDay, total))
	fprintWithTabs(writer, "  past week", offlineWeek, percentage(offlineWeek, total))
	fprintWithTabs(writer, "  past month", offlineMonth, percentage(offlineMonth, total))
	fprintWithTabs(writer, "Total", total, "100%")

	return nil
}

func (cmd status) Run(args []string) error {
	if len(args) == 0 {
		return cmd.printSummaryTable()
	}

	realCommand := NewDevices()
	for idx, arg := range args {
		if idx > 0 {
			fmt.Println()
		}

		var query []string
		if _, err := parseDeviceStatus(arg); err == nil {
			query = []string{"WHERE", "status", "is", arg, "ORDER", "BY", "status,id"}
		} else if strings.ContainsAny(arg, "./:") {
			query = []string{"WHERE", "ip", "in", arg, "ORDER", "BY", "ip,duration"}
		} else {
			query = []string{"WHERE", "id", "=", arg}
		}

		if err := realCommand.Run(query); err != nil {
			return err
		}
	}
	return nil
}
