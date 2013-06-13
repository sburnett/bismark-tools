package commands

import (
	"fmt"
	"github.com/sburnett/bismark-tools/bdm-client/datastore"
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
    OrderBy          datastore.Identifier
    Order            datastore.Order
    Limit            int
    NodeConstraint   string
    IpConstraint     string
    VersionConstraint string
    StatusConstraint *datastore.DeviceStatus
}

func parseDeviceQuery(query string) (*DeviceQuery, error) {
	queryParameters := DeviceQuery{
		OrderBy:          datastore.NodeId,
		Order:            datastore.Ascending,
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
            var statusConstraint datastore.DeviceStatus
            switch match {
            case "up", "online":
                statusConstraint = datastore.DeviceStatus(datastore.Online)
            case "stale", "late":
                statusConstraint = datastore.Stale
            case "down", "offline":
                statusConstraint = datastore.Offline
            default:
                return nil, fmt.Errorf("Invalid status: %s", match)
            }
            queryParameters.StatusConstraint = &statusConstraint
		case "node":
            queryParameters.NodeConstraint = match
		case "ip":
            queryParameters.IpConstraint = match
		case "version":
            queryParameters.VersionConstraint = match
		case "order":
			switch match {
			case "id", "node":
				queryParameters.OrderBy = datastore.NodeId
			case "ip", "address", "ip_address":
				queryParameters.OrderBy = datastore.IpAddress
			case "version", "bversion":
				queryParameters.OrderBy = datastore.Version
			case "last", "last_probe":
				queryParameters.OrderBy = datastore.LastProbe
			case "next", "next_probe":
				queryParameters.OrderBy = datastore.LastProbe
			case "outage", "duration", "outage_duration":
				queryParameters.OrderBy = datastore.OutageDuration
			default:
				return nil, fmt.Errorf("Invalid identifier: %s", match)
			}
		case "desc":
			switch match {
			case "asc":
				queryParameters.Order = datastore.Ascending
			case "desc":
				queryParameters.Order = datastore.Descending
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
