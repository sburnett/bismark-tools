package commands

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"os"
	"text/tabwriter"
)

type status struct{}

func NewStatus() BdmCommand {
	return new(status)
}

func (status) Name() string {
	return "status"
}

func (status) Description() string {
	return "Show whether a device is online"
}

func (status) Run(args []string) error {
	db, err := sql.Open("postgres", "")
	if err != nil {
		return fmt.Errorf("Error connecting to Postgres database: %s", err)
	}
	defer db.Close()

	if len(args) == 0 {
		return summarizeStatus(db)
	}

	queryString := `
        SELECT id, extract(epoch from date_trunc('second', date_last_seen - current_timestamp))
        FROM devices
        WHERE id LIKE $1`
	rows, err := db.Query(queryString, "%"+args[0])
	if err != nil {
		return fmt.Errorf("Error querying devices table: %s", err)
	}

	var nodeId, stateText string
	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer writer.Flush()
	rowCount := 0
	for rows.Next() {
		rowCount++

		if stateText != "" {
			fmt.Fprintln(os.Stderr, "That device ID is ambiguous")
			os.Exit(1)
		}
		var outageSeconds float64
		rows.Scan(&nodeId, &outageSeconds)

		deviceStatus := OutageDurationToDeviceStatus(outageSeconds)
		fprintWithTabs(writer, nodeId, deviceStatus)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("Error iterating through devices table: %s", err)
	}

	if rowCount == 0 {
		fmt.Fprintln(os.Stderr, "That device doesn't exist")
		os.Exit(1)
	}

	return nil
}
