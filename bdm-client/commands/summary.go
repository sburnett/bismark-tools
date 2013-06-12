package commands

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"time"
)

type Summary struct{}

func (name Summary) Name() string {
	return "summary"
}

func (name Summary) Description() string {
	return "Summarize the deployment"
}

func (name Summary) Run(args []string) error {
	db, err := sql.Open("postgres", "")
	if err != nil {
		return fmt.Errorf("Error connecting to Postgres database: %s", err)
	}
	defer db.Close()

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

	fmt.Printf("There are %d routers in the deployment. ", total)
	fmt.Printf("%d (%d%%) are online, %d (%d%%) are stale, and %d (%d%%) are offline.\n", online, int(float64(online)/float64(total)*100), stale, int(float64(stale)/float64(total)*100), offline, int(float64(offline)/float64(total)*100))

	return nil
}
