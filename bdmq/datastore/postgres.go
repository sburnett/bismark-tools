package datastore

import (
	"database/sql"
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/abh/geoip"
	_ "github.com/bmizerany/pq"
)

var geoipDatabase string

func init() {
	flag.StringVar(&geoipDatabase, "geoip_database", "/usr/share/GeoIP/GeoIP.dat", "Path of GeoIP database")
}

type PostgresDatastore struct {
	db         *sql.DB
	geolocator *geoip.GeoIP
}

func NewPostgresDatastore() (Datastore, error) {
	db, err := sql.Open("postgres", "")
	if err != nil {
		return nil, fmt.Errorf("Error connecting to Postgres database: %s", err)
	}

	geolocator, err := geoip.Open(geoipDatabase)
	if err != nil {
		return nil, err
	}

	return PostgresDatastore{db, geolocator}, nil
}

func (store PostgresDatastore) Close() {
	if err := store.db.Close(); err != nil {
		panic(err)
	}
}

func (store PostgresDatastore) SelectDevices(orderBy []Identifier, order []Order, limit int, nodeIdConstraint, ipAddressConstraint, countryCodeConstraint, versionConstraint string, deviceStatusConstraint *DeviceStatus) chan *DevicesResult {
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

			country, _ := store.geolocator.GetCountry(ipAddress)
			if country == "" {
				country = "??"
			}
			if countryCodeConstraint != "" && countryCodeConstraint != country {
				continue
			}

			outageDuration, err := time.ParseDuration(fmt.Sprintf("%ds", int(outageSeconds)))
			if err != nil {
				results <- &DevicesResult{Error: err}
			}
			results <- &DevicesResult{
				NodeId:             nodeId,
				IpAddress:          ipAddress,
				CountryCode:        country,
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

// A data structure to hold a key/value pair.
type pair struct {
	Key   string
	Value int
}

// A slice of Pairs that implements sort.Interface to sort by Value.
type pairList []pair

func (p pairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p pairList) Len() int           { return len(p) }
func (p pairList) Less(i, j int) bool { return p[i].Value > p[j].Value }

// A function to turn a map into a pairList, then sort and return it.
func sortMapByValue(m map[string]int) pairList {
	p := make(pairList, len(m))
	i := 0
	for k, v := range m {
		p[i] = pair{k, v}
		i++
	}
	sort.Sort(p)
	return p
}

func (store PostgresDatastore) SelectCountries() chan *CountriesResult {
	runQuery := func(results chan *CountriesResult) {
		defer close(results)

		deviceQuery := `
        SELECT ip,
               extract(epoch from date_trunc('second', current_timestamp - date_last_seen)) AS online
        FROM devices`
		rows, err := store.db.Query(deviceQuery)
		if err != nil {
			results <- &CountriesResult{Error: fmt.Errorf("Error querying devices table: %s", err)}
			return
		}

		countriesCount := make(map[string]int)
		onlineCount := make(map[string]int)
		for rows.Next() {
			var ipAddress string
			var online float64
			if err := rows.Scan(&ipAddress, &online); err != nil {
				results <- &CountriesResult{Error: fmt.Errorf("Error iterating through devices table: %s", err)}
				return
			}

			country, _ := store.geolocator.GetCountry(ipAddress)
			if country == "" {
				country = "??"
			}
			countriesCount[country]++
			if online <= 600 {
				onlineCount[country]++
			}
		}
		sortedCountriesCount := sortMapByValue(countriesCount)
		for _, entry := range sortedCountriesCount {
			results <- &CountriesResult{
				Country:     entry.Key,
				Count:       entry.Value,
				OnlineCount: onlineCount[entry.Key],
			}
		}
		if err := rows.Err(); err != nil {
			results <- &CountriesResult{Error: fmt.Errorf("Error iterating through devices table: %s", err)}
		}
	}

	resultsChan := make(chan *CountriesResult)
	go runQuery(resultsChan)
	return resultsChan
}
