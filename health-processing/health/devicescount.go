package health

import (
	"strconv"
	"strings"

	"github.com/sburnett/bismark-tools/common"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func DevicesCountPipeline(levelDbManager store.Manager) transformer.Pipeline {
	logsStore := levelDbManager.Seeker("logs")
	devicesCountStore := levelDbManager.Writer("devices-count")
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "ExtractEthernetCount",
			Reader:      ReadOnlySomeLogs(logsStore, "swconfig_ports"),
			Transformer: transformer.MakeDoFunc(extractEthernetCount),
			Writer:      devicesCountStore,
		},
		transformer.PipelineStage{
			Name:        "ExtractWirelessCount",
			Reader:      ReadOnlySomeLogs(logsStore, "iw_station_count"),
			Transformer: transformer.MakeDoFunc(extractWirelessCount),
			Writer:      devicesCountStore,
		},
	}
}

func extractEthernetCount(record *store.Record, outputChan chan *store.Record) {
	var logKey common.LogKey
	lex.DecodeOrDie(record.Key, &logKey)

	var deviceCount int64
	lines := strings.Split(string(record.Value), "\n")
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		words := strings.Fields(line)
		if len(words) < 2 {
			continue
		}
		if words[1] != "link:up" {
			continue
		}
		deviceCount++
	}
	outputChan <- &store.Record{
		Key:   lex.EncodeOrDie(logKey.Node, "ethernet", logKey.Timestamp),
		Value: lex.EncodeOrDie(deviceCount),
	}
}

func extractWirelessCount(record *store.Record, outputChan chan *store.Record) {
	var logKey common.LogKey
	lex.DecodeOrDie(record.Key, &logKey)

	lines := strings.Split(string(record.Value), "\n")
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		words := strings.Fields(line)
		if len(words) < 2 {
			continue
		}
		interfaceName := strings.TrimSuffix(words[0], ":")
		deviceCount, err := strconv.ParseInt(words[1], 10, 64)
		if err != nil {
			continue
		}
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(logKey.Node, interfaceName, logKey.Timestamp),
			Value: lex.EncodeOrDie(deviceCount),
		}
	}
}
