package commands

import (
	"github.com/sburnett/bismark-tools/bdm-client/datastore"
	"os"
	"strings"
	"text/tabwriter"
)

type devices struct{}

func NewDevices() BdmCommand {
	return new(devices)
}

func (devices) Name() string {
	return "devices"
}

func (devices) Description() string {
	return "Query device information"
}

func (devices) Run(args []string) error {
	db, err := datastore.NewPostgresDatastore()
	if err != nil {
		return err
	}
	defer db.Close()

	params, err := parseDeviceQuery(strings.Join(args, " "))
	if err != nil {
		return err
	}

	results := db.SelectDevices(params.OrderBy, params.Order, params.Limit, params.NodeLike, params.IpWithin, params.VersionEquals, params.StatusEquals)
	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer writer.Flush()
	fprintWithTabs(writer, "NODE ID", "IP ADDRESS", "VERSION", "LAST PROBE", "STATUS", "NEXT PROBE", "OUTAGE DURATION")
	for r := range results {
		if r.Error != nil {
			return r.Error
		}

		lastSeenText := r.LastSeen.Format("2006-01-02 15:04:05")

		var nextPingText string
		switch r.DeviceStatus {
		case datastore.Online:
			nextPingText = r.NextProbe.String()
		case datastore.Stale:
			nextPingText = "late"
		case datastore.Offline:
			nextPingText = "unknown"
		}

		fprintWithTabs(writer, r.NodeId, r.IpAddress, r.Version, lastSeenText, r.DeviceStatus, nextPingText, r.OutageDurationText)
	}

	return nil
}
