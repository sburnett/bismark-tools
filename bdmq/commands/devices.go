package commands

import (
	"os"
	"strings"
	"text/tabwriter"

	"github.com/sburnett/bismark-tools/bdmq/datastore"
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

	if len(params.Order) == 0 {
		params.OrderBy = append(params.OrderBy, datastore.NodeId)
		params.Order = append(params.Order, datastore.Ascending)
	}

	results := db.SelectDevices(params.OrderBy, params.Order, params.Limit, params.NodeLike, params.IpWithin, params.CountryCode, params.VersionEquals, params.StatusEquals)
	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer writer.Flush()
	fprintWithTabs(writer, "NODE ID", "IP ADDRESS", "COUNTRY", "VERSION", "LAST PROBE", "STATUS", "OUTAGE DURATION")
	for r := range results {
		if r.Error != nil {
			return r.Error
		}

		lastSeenText := r.LastSeen.Format("2006-01-02 15:04:05")

		fprintWithTabs(writer, r.NodeId, r.IpAddress, r.CountryCode, r.Version, lastSeenText, r.DeviceStatus, r.OutageDurationText)
	}

	return nil
}
