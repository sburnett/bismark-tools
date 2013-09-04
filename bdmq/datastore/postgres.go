package datastore

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"strings"
	"time"
)

type PostgresDatastore struct {
	db *sql.DB
}

func NewPostgresDatastore() (Datastore, error) {
	db, err := sql.Open("postgres", "")
	if err != nil {
		return nil, fmt.Errorf("Error connecting to Postgres database: %s", err)
	}
	return PostgresDatastore{db}, nil
}

func (store PostgresDatastore) Close() {
	if err := store.db.Close(); err != nil {
		panic(err)
	}
}

func (store PostgresDatastore) SelectDevices(orderBy []Identifier, order []Order, limit int, nodeIdConstraint, ipAddressConstraint, versionConstraint string, deviceStatusConstraint *DeviceStatus) chan *DevicesResult {
	runQuery := func(results chan *DevicesResult) {
		defer close(results)

		var whereConstraints []string
		if nodeIdConstraint != "" {
			whereConstraints = append(whereConstraints, fmt.Sprintf("id ILIKE '%%%s'", nodeIdConstraint))
		}
		if ipAddressConstraint != "" {
			whereConstraints = append(whereConstraints, fmt.Sprintf("ip <<= '%s'", ipAddressConstraint))
		}
		if versionConstraint != "" {
			whereConstraints = append(whereConstraints, fmt.Sprintf("bversion = '%s'", versionConstraint))
		}
		var whereClause string
		if len(whereConstraints) > 0 {
			whereClause = fmt.Sprint("WHERE ", strings.Join(whereConstraints, " AND "))
		}
		var orderByClause string
		if len(orderBy) > 0 {
			var orderConstraints []string
			for idx, ident := range orderBy {
				orderConstraints = append(orderConstraints, fmt.Sprint(ident, " ", order[idx]))
			}
			orderByClause = fmt.Sprint("ORDER BY ", strings.Join(orderConstraints, ", "))
		}
		queryString := `
            SELECT
                id AS node,
                ip,
                bversion AS version,
                date_last_seen AS last_probe,
                extract(epoch from current_timestamp - date_last_seen) AS outage_seconds,
                date_trunc('second', age(current_timestamp, date_last_seen)) AS outage_duration
            FROM devices
            %s
            %s`
		preparedQueryString := fmt.Sprintf(queryString, whereClause, orderByClause)
		rows, err := store.db.Query(preparedQueryString)
		if err != nil {
			results <- &DevicesResult{Error: fmt.Errorf("Error querying devices table: %s", err)}
			return
		}

		rowCount := 0
		for rows.Next() {
			if limit > 0 && rowCount >= limit {
				break
			}

			var (
				nodeId, ipAddress, version string
				lastSeen                   time.Time
				outageSeconds              float64
				outageDurationText         string
			)
			if err := rows.Scan(&nodeId, &ipAddress, &version, &lastSeen, &outageSeconds, &outageDurationText); err != nil {
				results <- &DevicesResult{Error: fmt.Errorf("Error querying devices table: %s", err)}
				return
			}

			deviceStatus := OutageDurationToDeviceStatus(outageSeconds)
			if deviceStatusConstraint != nil && *deviceStatusConstraint != deviceStatus {
				continue
			}

			outageDuration, err := time.ParseDuration(fmt.Sprintf("%ds", int(outageSeconds)))
			if err != nil {
				results <- &DevicesResult{Error: err}
			}
			results <- &DevicesResult{
				NodeId:             nodeId,
				IpAddress:          ipAddress,
				Version:            version,
				LastSeen:           lastSeen,
				DeviceStatus:       deviceStatus,
				OutageDuration:     outageDuration,
				NextProbe:          outageDurationToNextProbe(outageDuration),
				OutageDurationText: outageDurationText,
			}
			rowCount++
		}
		if err := rows.Err(); err != nil {
			results <- &DevicesResult{Error: fmt.Errorf("Error iterating through devices table: %s", err)}
		}
	}

	resultsChan := make(chan *DevicesResult)
	go runQuery(resultsChan)
	return resultsChan
}

func (store PostgresDatastore) SelectVersions() chan *VersionsResult {
	runQuery := func(results chan *VersionsResult) {
		defer close(results)

		versionQuery := `
        SELECT bversion,
               count(case when extract(epoch from date_trunc('second', current_timestamp - date_last_seen)) < 600 then 1 else null end) AS online,
               count(1) total
        FROM devices
        GROUP BY bversion
        ORDER BY total DESC`
		rows, err := store.db.Query(versionQuery)
		if err != nil {
			results <- &VersionsResult{Error: fmt.Errorf("Error querying devices table: %s", err)}
			return
		}

		for rows.Next() {
			var result VersionsResult
			if err := rows.Scan(&result.Version, &result.OnlineCount, &result.Count); err != nil {
				results <- &VersionsResult{Error: fmt.Errorf("Error iterating through devices table: %s", err)}
				return
			}
			results <- &result
		}
		if err := rows.Err(); err != nil {
			results <- &VersionsResult{Error: fmt.Errorf("Error iterating through devices table: %s", err)}
		}
	}

	resultsChan := make(chan *VersionsResult)
	go runQuery(resultsChan)
	return resultsChan
}
