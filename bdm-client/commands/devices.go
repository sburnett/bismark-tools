package commands

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"os"
	"regexp"
	"strings"
	"text/tabwriter"
	"time"
)

type Devices struct{}

func (name Devices) Name() string {
	return "devices"
}

func (name Devices) Description() string {
	return "List BISmark devices"
}

func matchArguments(args []string) ([]string, []string) {
	statuses := `up|online|stale|late|down|offline`
	statusFilter := `status (?:=|is) (?P<status>` + statuses + `)`
	nodeFilter := `(?:node|id|node_id) (?:=|is|like) (?P<node>[a-z0-9]+)`
	ipFilter := `(?:ip|address|ip_address) (?:=|is|in) (?P<ip>[0-9a-f.:/]+)`
	versionFilter := `(?:version|bversion) (?:=|is) (?P<version>[0-9.\-]+)`
	wherePattern := `where (?:` + statusFilter + `|` + nodeFilter + `|` + ipFilter + `|` + versionFilter + `)`
	variables := `id|node|ip|address|ip_address|bversion|version|last|last_probe|next|next_probe|outage|duration|outage_duration`
	orderPattern := `order by (?P<order>` + variables + `)(?: (?P<desc>desc|asc))?`
	limitPattern := `limit (?P<limit>\d+)`
	argsPattern := "^(?:" + wherePattern + ")? *(?:" + orderPattern + ")? *(?:" + limitPattern + ")?$"
	matcher := regexp.MustCompile(argsPattern)
	matches := matcher.FindStringSubmatch(strings.ToLower(strings.Join(args, " ")))
	return matches, matcher.SubexpNames()
}

func secondsToDurationString(seconds float64) string {
	return (time.Second * time.Duration(seconds)).String()
}

func (name Devices) Run(args []string) error {
	db, err := sql.Open("postgres", "")
	if err != nil {
		return fmt.Errorf("Error connecting to Postgres database: %s", err)
	}
	defer db.Close()

	matches, names := matchArguments(args)
	if matches == nil {
		return fmt.Errorf("Invalid query")
	}

	order := "id"
	desc := "ASC"
	var limit int
	var statusConstraint *DeviceStatus
	whereClause := ""

	for idx, match := range matches {
		switch names[idx] {
		case "status":
			if match == "" {
				continue
			}
			status, err := ParseDeviceStatus(match)
			if err != nil {
				return fmt.Errorf("Query error: %s", err)
			}
			statusConstraint = &status
		case "node":
			if match != "" {
				whereClause = fmt.Sprintf("WHERE id ILIKE '%%%s'", match)
			}
		case "ip":
			if match != "" {
				whereClause = fmt.Sprintf("WHERE ip <<= '%s'", match)
			}
		case "version":
			if match != "" {
				whereClause = fmt.Sprintf("WHERE bversion = '%s'", match)
			}
		case "order":
			switch match {
			case "id", "node":
				order = "id"
			case "ip", "address", "ip_address":
				order = "ip"
			case "version", "bversion":
				order = "bversion"
			case "last", "last_probe":
				order = "date_last_seen"
			case "next", "next_probe":
				order = "date_last_seen"
			case "outage", "duration", "outage_duration":
				order = "outage_duration"
			}
		case "desc":
			switch match {
			case "asc":
				desc = "ASC"
			case "desc":
				desc = "DESC"
			}
		case "limit":
			if match != "" {
				fmt.Sscan(match, &limit)
			}
		}
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
	preparedQueryString := fmt.Sprintf(queryString, whereClause, order, desc)
	rows, err := db.Query(preparedQueryString)
	if err != nil {
		return fmt.Errorf("Error querying devices table: %s", err)
	}

	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer writer.Flush()
	fprintWithTabs(writer, "NODE ID", "IP ADDRESS", "VERSION", "LAST PROBE", "STATUS", "NEXT PROBE", "OUTAGE DURATION")
	rowsWritten := 0
	for rows.Next() {
		if limit > 0 && rowsWritten >= limit {
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

		if statusConstraint != nil && *statusConstraint != deviceStatus {
			continue
		}

		var nextPingText string
		switch deviceStatus {
		case Online:
			nextPingText = secondsToDurationString(OutageDurationToNextProbe(outageSeconds))
		case Stale:
			nextPingText = "soon"
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
