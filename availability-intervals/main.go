package main

import (
	"database/sql"
	"encoding/json"
	"expvar"
	"flag"
	_ "github.com/bmizerany/pq"
	"os"
	"time"
)

var outageThreshold time.Duration
var maxDays int
var outputFile string

var rowsProcessed, intervalsCreated *expvar.Int

func init() {
	flag.DurationVar(&outageThreshold, "outage_threshold", 5*time.Minute, "Trigger an outage when the duration between two pings from a router is longer than this threshold.")
	flag.IntVar(&maxDays, "max_days", 100, "Compute at most this many days of availability.")
	flag.StringVar(&outputFile, "output_file", "/tmp/bismark-availability.json", "Write avilability to this file in JSON format")
	flag.Parse()

	rowsProcessed = expvar.NewInt("RowsProcessed")
	intervalsCreated = expvar.NewInt("IntervalsCreated")
}

func daysToDuration(days int) time.Duration {
	return time.Duration(days) * time.Hour * time.Duration(-24)
}

func writeIntervals(intervalStarts, intervalEnds map[string][]int64, outputFile string) error {
	intervals := make(map[string][][]int64)
	for nodeId := range intervalStarts {
		intervals[nodeId] = [][]int64{intervalStarts[nodeId], intervalEnds[nodeId]}
	}

	var resultTuple []interface{}
	resultTuple = append(resultTuple, intervals)
	resultTuple = append(resultTuple, time.Now().Unix()*1000)
	output, err := json.Marshal(resultTuple)
	if err != nil {
		return err
	}

	intervalsFile, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer intervalsFile.Close()

	if _, err := intervalsFile.Write(output); err != nil {
		return err
	}

	return nil
}

func main() {
	currentStarts := make(map[string]*time.Time)
	currentEnds := make(map[string]*time.Time)
	intervalStarts := make(map[string][]int64)
	intervalEnds := make(map[string][]int64)

	db, err := sql.Open("postgres", "")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	maxDuration := daysToDuration(maxDays)
	minTime := time.Now().Add(maxDuration)
	rows, err := db.Query("SELECT date_seen, id FROM devices_log WHERE date_seen > $1 ORDER BY date_seen", minTime)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var dateSeen time.Time
		var nodeId string
		rows.Scan(&dateSeen, &nodeId)

		if currentEnds[nodeId] != nil && dateSeen.Sub(*currentEnds[nodeId]) > outageThreshold {
			intervalStarts[nodeId] = append(intervalStarts[nodeId], currentStarts[nodeId].Unix()*1000)
			intervalEnds[nodeId] = append(intervalEnds[nodeId], currentEnds[nodeId].Unix()*1000)
			intervalsCreated.Add(int64(1))
			currentStarts[nodeId] = nil
		}
		if currentStarts[nodeId] == nil {
			currentStarts[nodeId] = &dateSeen
		}
		currentEnds[nodeId] = &dateSeen

		rowsProcessed.Add(int64(1))
	}
	for nodeId := range currentStarts {
		intervalStarts[nodeId] = append(intervalStarts[nodeId], currentStarts[nodeId].Unix()*1000)
		intervalEnds[nodeId] = append(intervalEnds[nodeId], currentEnds[nodeId].Unix()*1000)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}

	if err := writeIntervals(intervalStarts, intervalEnds, outputFile); err != nil {
		panic(err)
	}
}
