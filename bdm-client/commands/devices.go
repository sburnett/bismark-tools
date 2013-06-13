package commands

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"os"
	"strings"
	"text/tabwriter"
	"time"
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
	db, err := sql.Open("postgres", "")
	if err != nil {
		return fmt.Errorf("Error connecting to Postgres database: %s", err)
	}
	defer db.Close()

	parameters, err := parseDeviceQuery(strings.Join(args, " "))
	if err != nil {
		return err
	}

	queryString := `
        SELECT
            id AS node,
            ip,
            bversion AS version,
            date_trunc('second', date_last_seen) AS last_probe,
            date_trunc('second', age(current_timestamp, date_last_seen)) AS outage_duration,
            extract(epoch from current_timestamp - date_last_seen) AS outage_seconds
        FROM devices
        %s
        ORDER BY %s %s`
	preparedQueryString := fmt.Sprintf(queryString, parameters.WhereClause, parameters.OrderBy, parameters.Order)
	rows, err := db.Query(preparedQueryString)
	if err != nil {
		return fmt.Errorf("Error querying devices table: %s", err)
	}

	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer writer.Flush()
	fprintWithTabs(writer, "NODE ID", "IP ADDRESS", "VERSION", "LAST PROBE", "STATUS", "NEXT PROBE", "OUTAGE DURATION")
	rowsWritten := 0
	for rows.Next() {
		if parameters.Limit >= 0 && rowsWritten >= parameters.Limit {
			break
		}

		var (
			nodeId, ipAddress, version string
			lastSeen                   time.Time
			outageDuration             string
			outageSeconds              float64
		)
		rows.Scan(&nodeId, &ipAddress, &version, &lastSeen, &outageDuration, &outageSeconds)

		deviceStatus := OutageDurationToDeviceStatus(outageSeconds)

		if parameters.StatusConstraint != nil && *parameters.StatusConstraint != deviceStatus {
			continue
		}

		var nextPingText string
		switch deviceStatus {
		case Online:
			nextPingText = secondsToDurationString(OutageDurationToNextProbe(outageSeconds))
		case Stale:
			nextPingText = "late"
		case Offline:
			nextPingText = ""
		}

		fprintWithTabs(writer, nodeId, ipAddress, version, lastSeen.Format("2006-01-02 15:04:05"), deviceStatus, nextPingText, outageDuration)
		rowsWritten++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("Error iterating through devices table: %s", err)
	}

	return nil
}
