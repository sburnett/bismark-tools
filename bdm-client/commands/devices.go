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
	wherePattern := `where (?:status (?:=|is) (?P<status>` + statuses + `)|node like (?P<node>\S+))`
	variables := `id|node|ip|bversion|version|last|last_ping|next|next_ping|outage|duration|outage_duration`
	orderPattern := `order by (?P<order>` + variables + `)(?: (?P<desc>desc|asc))?`
	limitPattern := `limit (?P<limit>\d+)`
	argsPattern := "^(?:" + wherePattern + ")? *(?:" + orderPattern + ")? *(?:" + limitPattern + ")?$"
	matcher := regexp.MustCompile(argsPattern)
	matches := matcher.FindStringSubmatch(strings.ToLower(strings.Join(args, " ")))
	return matches, matcher.SubexpNames()
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
	statusConstraint := ""
	whereClause := ""

	for idx, match := range matches {
		switch names[idx] {
		case "status":
			switch match {
			case "up", "online":
				statusConstraint = "up"
			case "down", "offline":
				statusConstraint = "down"
			case "stale", "late":
				statusConstraint = "stale"
			}
		case "node":
			if match != "" {
				whereClause = fmt.Sprintf("WHERE id ILIKE '%%%s'", match)
			}
		case "order":
			switch match {
			case "id", "node":
				order = "id"
			case "ip":
				order = "ip"
			case "version", "bversion":
				order = "bversion"
			case "last", "last_ping":
				order = "date_last_seen"
			case "next", "next_ping":
				order = "next_probe"
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
            extract(epoch from date_trunc('second', date_last_seen + '60 seconds' - current_timestamp)) AS next_probe,
            date_trunc('second', age(current_timestamp, date_last_seen)) AS outage_duration
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

		var nodeId, ipAddress string
		var version int
		var lastSeen time.Time
		var nextPingSeconds float64
		var outageDuration string
		rows.Scan(&nodeId, &ipAddress, &version, &lastSeen, &nextPingSeconds, &outageDuration)

		var statusText string
		switch {
		case lastSeen.IsZero():
			statusText = "down"
		case nextPingSeconds > 0:
			statusText = "up"
		case nextPingSeconds < -540:
			statusText = "down"
		default:
			statusText = "stale"
		}
		if statusConstraint != "" && statusText != statusConstraint {
			continue
		}

		var nextPingText string
		switch {
		case nextPingSeconds > 0:
			nextPingText = (time.Second * time.Duration(nextPingSeconds)).String()
		case nextPingSeconds < -540:
			nextPingText = ""
		case lastSeen.IsZero():
			nextPingText = ""
		default:
			nextPingText = "soon"
		}

		fprintWithTabs(writer, nodeId, ipAddress, version, lastSeen.Format("2006-01-02 15:04:05"), statusText, nextPingText, outageDuration)

		rowsWritten++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("Error iterating through devices table: %s", err)
	}

	return nil
}
