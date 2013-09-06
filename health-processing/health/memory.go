package health

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func MemoryUsagePipeline(levelDbManager, csvManager store.Manager) transformer.Pipeline {
	logsStore := levelDbManager.Seeker("logs")
	memoryUsageStore := levelDbManager.ReadingWriter("memory")
	var node string
	var timestamp, used, free int64
	csvStore := csvManager.Writer("memory.csv", []string{"node", "timestamp"}, []string{"used", "free"}, &node, &timestamp, &used, &free)
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "Memory",
			Reader:      ReadOnlySomeLogs(logsStore, "top"),
			Transformer: transformer.MakeDoFunc(extractMemoryUsage),
			Writer:      memoryUsageStore,
		},
		transformer.PipelineStage{
			Name:   "WriteMemoryUsageCsv",
			Reader: memoryUsageStore,
			Writer: csvStore,
		},
	}
}

func parseMemoryString(usageString string) (int64, error) {
	if usageString[len(usageString)-1] != 'K' {
		log.Println("Invalid memory unit")
		return 0, fmt.Errorf("Invalid memory unit")
	}
	used, err := strconv.Atoi(usageString[:len(usageString)-1])
	if err != nil {
		log.Println("Error parsing float: %v", err)
		return 0, fmt.Errorf("Error parsing float: %v", err)
	}
	return int64(used), nil
}

func extractMemoryUsage(record *store.Record, outputChan chan *store.Record) {
	var logKey LogKey
	lex.DecodeOrDie(record.Key, &logKey)

	lines := strings.Split(string(record.Value), "\n")
	if len(lines) < 1 {
		log.Println("Not enough lines")
		return
	}
	words := strings.Split(lines[0], " ")
	if len(words) < 4 {
		log.Println("Not enough words")
		return
	}
	if words[0] != "Mem:" {
		log.Println("Expected line beginning with 'Mem:'")
		return
	}
	used, err := parseMemoryString(words[1])
	if err != nil {
		return
	}
	free, err := parseMemoryString(words[3])
	if err != nil {
		return
	}
	outputChan <- &store.Record{
		Key:   lex.EncodeOrDie(logKey.Node, logKey.Timestamp),
		Value: lex.EncodeOrDie(used, free),
	}
}
