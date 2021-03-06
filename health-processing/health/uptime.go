package health

import (
	"log"
	"strconv"
	"strings"

	"github.com/sburnett/bismark-tools/common"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func UptimePipeline(levelDbManager, csvManager, sqliteManager store.Manager) transformer.Pipeline {
	logsStore := levelDbManager.Seeker("logs")
	uptimeStore := levelDbManager.ReadingWriter("uptime")
	var node string
	var timestamp, uptime int64
	csvStore := csvManager.Writer("uptime.csv", []string{"node", "timestamp"}, []string{"uptime"}, &node, &timestamp, &uptime)
	sqliteStore := sqliteManager.Writer("uptime", []string{"node", "timestamp"}, []string{"uptime"}, &node, &timestamp, &uptime)
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "Uptime",
			Reader:      ReadOnlySomeLogs(logsStore, "uptime"),
			Transformer: transformer.MakeMapFunc(extractUptime),
			Writer:      uptimeStore,
		},
		transformer.PipelineStage{
			Name:   "WriteUptimeCsv",
			Reader: uptimeStore,
			Writer: csvStore,
		},
		transformer.PipelineStage{
			Name:   "WriteUptimeSqlite",
			Reader: uptimeStore,
			Writer: sqliteStore,
		},
	}
}

func extractUptime(record *store.Record) *store.Record {
	var logKey common.LogKey
	lex.DecodeOrDie(record.Key, &logKey)

	lines := strings.Split(string(record.Value), "\n")
	if len(lines) < 2 {
		log.Println("Not enough lines")
		return nil
	}
	words := strings.Split(lines[1], " ")
	uptimeSeconds, err := strconv.ParseFloat(words[0], 64)
	if err != nil {
		log.Println("Error parsing float: %v", err)
		return nil
	}
	return &store.Record{
		Key:   lex.EncodeOrDie(logKey.Node, logKey.Timestamp),
		Value: lex.EncodeOrDie(int64(uptimeSeconds)),
	}
}
