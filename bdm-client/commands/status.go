package commands

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"os"
	"time"
)

type Status struct{}

func (name Status) Name() string {
	return "status"
}

func (name Status) Description() string {
	return "Check whether a BISmark router is online"
}

func (name Status) Run(args []string) error {
	db, err := sql.Open("postgres", "")
	if err != nil {
		return fmt.Errorf("Error connecting to Postgres database: %s", err)
	}
	defer db.Close()

	queryString := `
        SELECT
            id,
            extract(epoch from date_trunc('second', date_last_seen - current_timestamp)) AS last_probe_seconds,
            date_trunc('second', date_last_seen) AS last_probe
        FROM devices
        WHERE id LIKE $1`
	rows, err := db.Query(queryString, "%"+args[0])
	if err != nil {
		return fmt.Errorf("Error querying devices table: %s", err)
	}

	var nodeId, stateText string
	for rows.Next() {
		if stateText != "" {
			fmt.Fprintln(os.Stderr, "That node ID is ambiguous")
			os.Exit(1)
		}
		var lastProbeSeconds float64
		var lastProbe time.Time
		rows.Scan(&nodeId, &lastProbeSeconds, &lastProbe)

		switch {
		case lastProbe.IsZero():
			stateText = "down"
		case lastProbeSeconds <= 60:
			stateText = "up"
		case lastProbeSeconds > 1200:
			stateText = "down"
		default:
			stateText = "stale"
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("Error iterating through devices table: %s", err)
	}

	if stateText == "" {
		fmt.Fprintln(os.Stderr, "That node ID doesn't exist")
		os.Exit(1)
	}

	fmt.Println(nodeId, stateText)

	return nil
}
