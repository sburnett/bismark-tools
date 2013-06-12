package commands

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"os"
	"time"
)

type Info struct{}

func (name Info) Name() string {
	return "info"
}

func (name Info) Description() string {
	return "Show detailed information about a single device"
}

func (name Info) Run(args []string) error {
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
            extract(epoch from date_trunc('second', date_last_seen + '60 seconds' - current_timestamp)) AS next_probe,
            date_trunc('second', age(current_timestamp, date_last_seen)) AS outage_duration
        FROM devices
        WHERE id LIKE $1`
	rows, err := db.Query(queryString, "%"+args[0])
	if err != nil {
		return fmt.Errorf("Error querying devices table: %s", err)
	}

	var nodeId, ipAddress string
	var version int
	var lastSeen time.Time
	var nextPingSeconds float64
	var outageDuration string
	rowCount := 0
	for rows.Next() {
		rowCount++
		if rowCount > 1 {
			fmt.Fprintln(os.Stderr, "That device ID is ambiguous")
			os.Exit(1)
		}
		rows.Scan(&nodeId, &ipAddress, &version, &lastSeen, &nextPingSeconds, &outageDuration)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("Error iterating through devices table: %s", err)
	}
	if rowCount == 0 {
		fmt.Fprintf(os.Stderr, "Device %s doesn't exist\n", args[0])
		os.Exit(1)
	}

	switch {
	case lastSeen.IsZero():
		fmt.Printf("Device %s has never sent any probes.\n", nodeId, outageDuration, lastSeen)
	case nextPingSeconds >= 0:
		fmt.Printf("Device %s is online and should be sending another probe in about %s.\n", nodeId, (time.Second * time.Duration(nextPingSeconds)))
	case nextPingSeconds < -600:
		fmt.Printf("Device %s has been offline for %s, since %s.\n", nodeId, outageDuration, lastSeen)
	default:
		fmt.Printf("Device %s is about %s late sending its next probe.\n", nodeId, (time.Second * time.Duration(-1*nextPingSeconds)))
	}

	return nil
}
