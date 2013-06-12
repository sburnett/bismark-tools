package commands

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"os"
	"text/tabwriter"
	"time"
)

type Summary struct{}

func (name Summary) Name() string {
	return "summary"
}

func (name Summary) Description() string {
	return "Summarize the deployment"
}

func summarizeStatus(db *sql.DB) error {
	queryString := `
        SELECT
            id AS node,
            ip,
            bversion AS version,
            date_trunc('second', date_last_seen) AS last_probe,
            extract(epoch from date_trunc('second', current_timestamp - date_last_seen)) AS outage_seconds
        FROM devices`
	rows, err := db.Query(queryString)
	if err != nil {
		return fmt.Errorf("Error querying devices table: %s", err)
	}

	var total, online, stale, offline int
	for rows.Next() {
		var nodeId, ipAddress string
		var version int
		var lastProbe time.Time
		var outageSeconds float64
		rows.Scan(&nodeId, &ipAddress, &version, &lastProbe, &outageSeconds)

		switch {
		case lastProbe.IsZero():
			offline++
		case outageSeconds <= 60:
			online++
		case outageSeconds > 600:
			offline++
		default:
			stale++
		}
		total++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("Error iterating through devices table: %s", err)
	}

	offlineQuery := `
        SELECT sum(case when extract(epoch from date_trunc('second', current_timestamp - date_last_seen)) > 86400 then 1 else 0 end),
               sum(case when extract(epoch from date_trunc('second', current_timestamp - date_last_seen)) > 7 * 86400 then 1 else 0 end),
               sum(case when extract(epoch from date_trunc('second', current_timestamp - date_last_seen)) > 30 * 86400 then 1 else 0 end)
        FROM devices`
	offlineRow := db.QueryRow(offlineQuery)
	if offlineRow == nil {
		return fmt.Errorf("Error querying devices table")
	}

	var offlineDay, offlineWeek, offlineMonth int
	offlineRow.Scan(&offlineDay, &offlineWeek, &offlineMonth)

	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer writer.Flush()
	fprintWithTabs(writer, "DEVICE STATUS", "COUNT", "PERCENTAGE")
	fprintWithTabs(writer, "Online", online, fmt.Sprintf("%d%%", int(float64(online)/float64(total)*100)))
	fprintWithTabs(writer, "Stale", stale, fmt.Sprintf("%d%%", int(float64(stale)/float64(total)*100)))
	fprintWithTabs(writer, "Offline", offline, fmt.Sprintf("%d%%", int(float64(offline)/float64(total)*100)))
	fprintWithTabs(writer, "  past day", offline-offlineDay, fmt.Sprintf("%d%%", int(float64(offline-offlineDay)/float64(total)*100)))
	fprintWithTabs(writer, "  past week", offline-offlineWeek, fmt.Sprintf("%d%%", int(float64(offline-offlineWeek)/float64(total)*100)))
	fprintWithTabs(writer, "  past month", offline-offlineMonth, fmt.Sprintf("%d%%", int(float64(offline-offlineMonth)/float64(total)*100)))
	fprintWithTabs(writer, "Total", total, "100%")

	return nil
}

func summarizeVersions(db *sql.DB) error {
	versionQuery := `
        SELECT bversion,
               count(case when extract(epoch from date_trunc('second', current_timestamp - date_last_seen)) < 600 then 1 else null end) AS online,
               count(1) total
        FROM devices
        GROUP BY bversion
        ORDER BY total DESC`
	rows, err := db.Query(versionQuery)
	if err != nil {
		return fmt.Errorf("Error querying devices table: %s", err)
	}

	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer writer.Flush()
	fprintWithTabs(writer, "VERSION", "TOTAL", "ONLINE")
	for rows.Next() {
		var version string
		var onlineCount, count int
		rows.Scan(&version, &onlineCount, &count)
		fprintWithTabs(writer, version, count, onlineCount)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("Error iterating through devices table: %s", err)
	}

	return nil
}

func (name Summary) Run(args []string) error {
	db, err := sql.Open("postgres", "")
	if err != nil {
		return fmt.Errorf("Error connecting to Postgres database: %s", err)
	}
	defer db.Close()

	if err := summarizeStatus(db); err != nil {
		return fmt.Errorf("Error summarizing deployment status: %s", err)
	}

	fmt.Println()
	if err := summarizeVersions(db); err != nil {
		return fmt.Errorf("Error summarizing versions: %s", err)
	}

	return nil
}
