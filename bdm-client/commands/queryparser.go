package commands

import (
	"fmt"
	"github.com/sburnett/bismark-tools/bdm-client/datastore"
	"regexp"
	"strconv"
	"strings"
)

func matchDeviceQuery(query string) ([]string, []string) {
	statusFilter := `(?:status|state) (?:=|is) (?P<status>[a-z]+)`
	nodeFilter := `(?:node|id|node_id) (?:=|is|like) (?P<node>[a-z0-9]+)`
	ipFilter := `(?:ip|address|ip_address) (?:=|is|in) (?P<ip>[0-9a-f.:/]+)`
	versionFilter := `(?:version|bversion) (?:=|is) (?P<version>[0-9.\-]+)`
	whereFilters := `(?:` + statusFilter + `|` + nodeFilter + `|` + ipFilter + `|` + versionFilter + `)`
	wherePatterns := `where (?:` + whereFilters + `)(?: and ` + whereFilters + `)*`
	orderPattern := `(?P<order>[a-z]+)(?: (?P<desc>desc|asc))?`
	orderPatterns := `order by (?:` + orderPattern + `)(?:, *` + orderPattern + `)*`
	limitPattern := `limit (?P<limit>\d+)`
	argsPattern := `^(?:` + wherePatterns + `)? *(?:` + orderPatterns + `)? *(?:` + limitPattern + `)?$`
	matcher := regexp.MustCompile(argsPattern)
	matches := matcher.FindStringSubmatch(strings.ToLower(query))
	return matches, matcher.SubexpNames()
}

type DeviceQuery struct {
	OrderBy       []datastore.Identifier
	Order         []datastore.Order
	Limit         int
	NodeLike      string
	IpWithin      string
	VersionEquals string
	StatusEquals  *datastore.DeviceStatus
}

func parseDeviceQuery(query string) (*DeviceQuery, error) {
	var queryParameters DeviceQuery

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
			statusConstraint, err := parseDeviceStatus(match)
			if err != nil {
				return nil, err
			}
			queryParameters.StatusEquals = &statusConstraint
		case "node":
			queryParameters.NodeLike = match
		case "ip":
			queryParameters.IpWithin = match
		case "version":
			queryParameters.VersionEquals = match
		case "order":
			orderBy, err := parseIdentifier(match)
			if err != nil {
				return nil, err
			}
			queryParameters.OrderBy = append(queryParameters.OrderBy, orderBy)
			queryParameters.Order = append(queryParameters.Order, datastore.Ascending)
		case "desc":
			order, err := parseOrder(match)
			if err != nil {
				return nil, err
			}
			queryParameters.Order[len(queryParameters.Order)-1] = order
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

func parseDeviceStatus(text string) (datastore.DeviceStatus, error) {
	switch text {
	case "up", "online", "on", "available":
		return datastore.Online, nil
	case "stale", "late":
		return datastore.Stale, nil
	case "down", "offline", "off", "unavailable":
		return datastore.Offline, nil
	default:
		return datastore.Offline, fmt.Errorf("Invalid status: %s", text)
	}
}

func parseIdentifier(text string) (datastore.Identifier, error) {
	switch text {
	case "id", "node":
		return datastore.NodeId, nil
	case "ip", "address", "ip_address":
		return datastore.IpAddress, nil
	case "version", "bversion":
		return datastore.Version, nil
	case "last", "last_probe":
		return datastore.LastProbe, nil
	case "next", "next_probe", "status":
		return datastore.LastProbe, nil
	case "outage", "duration", "outage_duration":
		return datastore.OutageDuration, nil
	default:
		return datastore.NodeId, fmt.Errorf("Invalid identifier: %s", text)
	}
}

func parseOrder(text string) (datastore.Order, error) {
	switch text {
	case "asc", "ascending":
		return datastore.Ascending, nil
	case "desc", "descending", "reverse":
		return datastore.Descending, nil
	default:
		return datastore.Ascending, fmt.Errorf("Invalid order constraint: %s", text)
	}
}
