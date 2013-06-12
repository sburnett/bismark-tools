package commands

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

func matchDeviceQuery(query string) ([]string, []string) {
	statuses := `up|online|stale|late|down|offline`
	statusFilter := `status (?:=|is) (?P<status>` + statuses + `)`
	nodeFilter := `(?:node|id|node_id) (?:=|is|like) (?P<node>[a-z0-9]+)`
	ipFilter := `(?:ip|address|ip_address) (?:=|is|in) (?P<ip>[0-9a-f.:/]+)`
	versionFilter := `(?:version|bversion) (?:=|is) (?P<version>[0-9.\-]+)`
	wherePattern := `where (?:` + statusFilter + `|` + nodeFilter + `|` + ipFilter + `|` + versionFilter + `)`
	variables := `id|node|ip|address|ip_address|bversion|version|last|last_probe|next|next_probe|outage|duration|outage_duration`
	orderPattern := `order by (?P<order>` + variables + `)(?: (?P<desc>desc|asc))?`
	limitPattern := `limit (?P<limit>\d+)`
	argsPattern := "^(?:" + wherePattern + ")? *(?:" + orderPattern + ")? *(?:" + limitPattern + ")?$"
	matcher := regexp.MustCompile(argsPattern)
	matches := matcher.FindStringSubmatch(strings.ToLower(query))
	return matches, matcher.SubexpNames()
}

type DeviceQuery struct {
	OrderBy          string
	Order            string
	Limit            int
	StatusConstraint *DeviceStatus
	WhereClause      string
}

func parseDeviceQuery(query string) (*DeviceQuery, error) {
	queryParameters := DeviceQuery{
		OrderBy:          "id",
		Order:            "ASC",
		Limit:            -1,
		StatusConstraint: nil,
		WhereClause:      "",
	}

	matches, names := matchDeviceQuery(query)
    if matches == nil {
        return nil, fmt.Errorf("Invalid query")
    }

	for idx, match := range matches {
		if match == "" {
			continue
		}
		switch names[idx] {
		case "status":
			status, err := ParseDeviceStatus(match)
			if err != nil {
				return nil, err
			}
			queryParameters.StatusConstraint = &status
		case "node":
			queryParameters.WhereClause = fmt.Sprintf("WHERE id ILIKE '%%%s'", match)
		case "ip":
			queryParameters.WhereClause = fmt.Sprintf("WHERE ip <<= '%s'", match)
		case "version":
			queryParameters.WhereClause = fmt.Sprintf("WHERE bversion = '%s'", match)
		case "order":
			switch match {
			case "id", "node":
				queryParameters.OrderBy = "id"
			case "ip", "address", "ip_address":
				queryParameters.OrderBy = "ip"
			case "version", "bversion":
				queryParameters.OrderBy = "bversion"
			case "last", "last_probe":
				queryParameters.OrderBy = "date_last_seen"
			case "next", "next_probe":
				queryParameters.OrderBy = "date_last_seen"
			case "outage", "duration", "outage_duration":
				queryParameters.OrderBy = "outage_duration"
			default:
				return nil, fmt.Errorf("Invalid identifier: %s", match)
			}
		case "desc":
			switch match {
			case "asc":
				queryParameters.Order = "ASC"
			case "desc":
				queryParameters.Order = "DESC"
			default:
				return nil, fmt.Errorf("Invalid order constraint: %s", match)
			}
		case "limit":
			limit, err := strconv.Atoi(match)
			if err != nil {
				return nil, err
			}
			queryParameters.Limit = limit
		}
	}
	return &queryParameters, nil
}
