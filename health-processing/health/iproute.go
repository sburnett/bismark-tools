package health

import (
	"log"
	"strings"

	"github.com/sburnett/bismark-tools/common"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func IpRoutePipeline(levelDbManager, sqliteManager store.Manager) transformer.Pipeline {
	logsStore := levelDbManager.Seeker("logs")
	defaultRoutesStore := levelDbManager.ReadingWriter("default-routes")
	var node string
	var timestamp int64
	var gateway string
	sqliteStore := sqliteManager.Writer("defaultroutes", []string{"node", "timestamp"}, []string{"gateway"}, &node, &timestamp, &gateway)
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "ExtractDefaultRoute",
			Reader:      ReadOnlySomeLogs(logsStore, "iproute"),
			Transformer: transformer.MakeDoFunc(extractDefaultRoute),
			Writer:      defaultRoutesStore,
		},
		transformer.PipelineStage{
			Name:   "WriteDefaultRoutesSqlite",
			Reader: defaultRoutesStore,
			Writer: sqliteStore,
		},
	}
}

func extractDefaultRoute(record *store.Record, outputChan chan *store.Record) {
	var logKey common.LogKey
	lex.DecodeOrDie(record.Key, &logKey)

	lines := strings.Split(string(record.Value), "\n")
	var ipAddress *string
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		words := strings.Fields(line)
		if len(words) < 3 {
			continue
		}
		if words[0] != "default" {
			continue
		}
		if ipAddress != nil {
			log.Println("Multiple default routes")
			continue
		}
		ipAddress = &words[2]
	}
	if ipAddress == nil {
		return
	}
	outputChan <- &store.Record{
		Key:   lex.EncodeOrDie(logKey.Node, logKey.Timestamp),
		Value: lex.EncodeOrDie(*ipAddress),
	}
}
