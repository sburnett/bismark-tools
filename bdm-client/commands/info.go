package commands

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
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
	db, err := sql.Open("postgres", "")
	if err != nil {
		return fmt.Errorf("Error connecting to Postgres database: %s", err)
	}
	defer db.Close()

	var pattern string
	if len(args) > 0 {
		pattern = args[0]
	}
	queryString := `
        SELECT
            id,
            ip,
            bversion,
            date_trunc('second', date_last_seen),
            date_trunc('second', age(current_timestamp, date_last_seen)),
            extract(epoch from date_trunc('second', current_timestamp - date_last_seen))
        FROM devices
        WHERE id LIKE $1`
	rows, err := db.Query(queryString, "%"+pattern)
	if err != nil {
		return fmt.Errorf("Error querying devices table: %s", err)
	}

	var nodeId, ipAddress string
	var version string
	var lastSeen time.Time
	var outageDuration string
	var outageSeconds float64
	var nodeIds []string
	rowCount := 0
	for rows.Next() {
		rowCount++
		rows.Scan(&nodeId, &ipAddress, &version, &lastSeen, &outageDuration, &outageSeconds)
		nodeIds = append(nodeIds, nodeId)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("Error iterating through devices table: %s", err)
	}
	if rowCount == 0 {
		fmt.Fprintf(os.Stderr, "Device %s doesn't exist\n", args[0])
		os.Exit(1)
	} else if rowCount > 1 {
		fmt.Fprintln(os.Stderr, "That device ID is ambiguous:", strings.Join(nodeIds, ", "))
		os.Exit(1)
	}

	deviceStatus := OutageDurationToDeviceStatus(outageSeconds)
	nextPing := OutageDurationToNextProbe(outageSeconds)
	switch deviceStatus {
	case Online:
		fmt.Printf("%s is online and should be sending its next probe in about %s.\n", nodeId, secondsToDurationString(nextPing))
	case Stale:
		fmt.Printf("%s is about %s late sending its next probe.\n", nodeId, secondsToDurationString(-1*nextPing))
	case Offline:
		fmt.Printf("%s has been offline for %s, since %s.\n", nodeId, outageDuration, lastSeen)
	}

	fmt.Printf("Its public IP address is %s and its firmware version is %s.\n", ipAddress, version)

	return nil
}
