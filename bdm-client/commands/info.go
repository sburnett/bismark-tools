package commands

import (
	"fmt"
	"github.com/sburnett/bismark-tools/bdm-client/datastore"
	"os"
	"strings"
	"time"
)

type info struct{}

func NewInfo() BdmCommand {
	return new(info)
}

func (info) Name() string {
	return "info"
}

func (info) Description() string {
	return "Show detailed information about a single device"
}

func (info) Run(args []string) error {
	db, err := datastore.NewPostgresDatastore()
	if err != nil {
		return fmt.Errorf("Error connecting to Postgres database: %s", err)
	}
	defer db.Close()

	var pattern string
	if len(args) > 0 {
		pattern = args[0]
	}
	var result *datastore.DevicesResult
	var nodeIds []string
	rowCount := 0
	for r := range db.SelectDevices(datastore.NodeId, datastore.Ascending, 0, pattern, "", "", nil) {
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
	}

	return nil
}
