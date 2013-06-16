package main

import (
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	_ "github.com/bmizerany/pq"
	"log"
	"os"
	"path/filepath"
	"time"
)

type availabilityInterval struct {
	StartTime, EndTime *time.Time
}

var outageThreshold time.Duration
var outputFile, cacheDirectory string
var minDate time.Time

var rowsProcessed, intervalsCreated *expvar.Int

func init() {
	flag.DurationVar(&outageThreshold, "outage_threshold", 5*time.Minute, "Trigger an outage when the duration between two pings from a router is longer than this threshold.")
	flag.StringVar(&outputFile, "output_file", "/tmp/bismark-availability.json", "Write avilability to this file in JSON format")
	flag.StringVar(&cacheDirectory, "cache_dir", "/tmp/bismark-availability-intervals", "Cache avilability intervals in this directory")
	var dateString string
	flag.StringVar(&dateString, "min_date", "2012-04-13", "Calculate intervals starting at this date")
	flag.Parse()

	dateParsed, err := time.Parse("2006-01-02", dateString)
	if err != nil {
		panic(fmt.Errorf("Invalid date %s: %s", dateString, err))
	}
	minDate = dateParsed

	rowsProcessed = expvar.NewInt("RowsProcessed")
	intervalsCreated = expvar.NewInt("IntervalsCreated")
}

func writeIntervals(availabilityIntervals map[string][]availabilityInterval, outputFile string) error {
	intervalsFile, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer intervalsFile.Close()

	encoder := gob.NewEncoder(intervalsFile)
	if err := encoder.Encode(availabilityIntervals); err != nil {
		return err
	}

	return nil
}

func processDay(db *sql.DB, startTime time.Time, filename string) error {
	endTime := startTime.AddDate(0, 0, 1)

	currentStarts := make(map[string]*time.Time)
	currentEnds := make(map[string]*time.Time)
	availabilityIntervals := make(map[string][]availabilityInterval)

	rows, err := db.Query("SELECT date_seen, id FROM devices_log WHERE date_seen >= $1 AND date_seen < $2 ORDER BY date_seen", startTime, endTime)
	if err != nil {
		return err
	}
	for rows.Next() {
		var dateSeen time.Time
		var nodeId string
		rows.Scan(&dateSeen, &nodeId)

		if currentEnds[nodeId] != nil && dateSeen.Sub(*currentEnds[nodeId]) > outageThreshold {
			currentInterval := availabilityInterval{currentStarts[nodeId], currentEnds[nodeId]}
			availabilityIntervals[nodeId] = append(availabilityIntervals[nodeId], currentInterval)
			currentStarts[nodeId] = nil
			intervalsCreated.Add(int64(1))
		}
		if currentStarts[nodeId] == nil {
			currentStarts[nodeId] = &dateSeen
		}
		currentEnds[nodeId] = &dateSeen

		rowsProcessed.Add(int64(1))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for nodeId := range currentStarts {
		currentInterval := availabilityInterval{currentStarts[nodeId], currentEnds[nodeId]}
		availabilityIntervals[nodeId] = append(availabilityIntervals[nodeId], currentInterval)
		intervalsCreated.Add(int64(1))
	}

	if err := writeIntervals(availabilityIntervals, filename); err != nil {
		return err
	}

	return nil
}

func coalesceIntervals(allIntervals map[string][]availabilityInterval) {
	for nodeId, intervals := range allIntervals {
		var lastInterval *availabilityInterval
		var newIntervals []availabilityInterval
		for _, interval := range intervals {
			if lastInterval != nil && interval.StartTime.Sub(*lastInterval.EndTime) <= outageThreshold {
				lastInterval.EndTime = interval.EndTime
			} else {
				newIntervals = append(newIntervals, interval)
				lastInterval = &newIntervals[len(newIntervals)-1]
			}
		}
		allIntervals[nodeId] = newIntervals
	}
}

func concatenateDailyIntervals(minDate, maxDate time.Time) (map[string][]availabilityInterval, error) {
	availabilityIntervals := make(map[string][]availabilityInterval)
	for currentDate := minDate; currentDate.Before(maxDate); currentDate = currentDate.AddDate(0, 0, 1) {
		var currentIntervals map[string][]availabilityInterval
		filename := filepath.Join(cacheDirectory, currentDate.Format("2006-01-02.gob"))
		intervalsFile, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		decoder := gob.NewDecoder(intervalsFile)
		if err := decoder.Decode(&currentIntervals); err != nil {
			return nil, err
		}
		for nodeId, intervals := range currentIntervals {
			availabilityIntervals[nodeId] = append(availabilityIntervals[nodeId], intervals...)
		}
		intervalsFile.Close()
	}
	coalesceIntervals(availabilityIntervals)
	return availabilityIntervals, nil
}

func writeAvailabilityJson(allIntervals map[string][]availabilityInterval) error {
	intervalStarts := make(map[string][]int64)
	intervalEnds := make(map[string][]int64)

	for nodeId, intervals := range allIntervals {
		for _, interval := range intervals {
			intervalStarts[nodeId] = append(intervalStarts[nodeId], interval.StartTime.Unix()*1000)
			intervalEnds[nodeId] = append(intervalEnds[nodeId], interval.EndTime.Unix()*1000)
		}
	}

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
	db, err := sql.Open("postgres", "")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	now := time.Now()
	maxDate := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	firstDate := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
	for currentDate := minDate; currentDate.Before(maxDate); currentDate = currentDate.AddDate(0, 0, 1) {
		filename := filepath.Join(cacheDirectory, currentDate.Format("2006-01-02.gob"))
		if _, err := os.Stat(filename); err != nil {
			firstDate = currentDate.AddDate(0, 0, -1)
			break
		}
	}

	for currentDate := firstDate; currentDate.Before(maxDate); currentDate = currentDate.AddDate(0, 0, 1) {
		log.Printf("Processing %s", currentDate.Format("2006-01-02"))
		filename := filepath.Join(cacheDirectory, currentDate.Format("2006-01-02.gob"))
		if err := processDay(db, currentDate, filename); err != nil {
			panic(err)
		}
	}

	allIntervals, err := concatenateDailyIntervals(minDate, maxDate)
	if err != nil {
		panic(err)
	}

	if err := writeAvailabilityJson(allIntervals); err != nil {
		panic(err)
	}
}
