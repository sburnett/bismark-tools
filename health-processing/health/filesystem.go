package health

import (
	"log"
	"strconv"
	"strings"

	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func FilesystemUsagePipeline(levelDbManager, csvManager store.Manager) transformer.Pipeline {
	logsStore := levelDbManager.Seeker("logs")
	filesystemUsageStore := levelDbManager.ReadingWriter("filesystem")
	var mount, node string
	var timestamp, used, free int64
	csvStore := csvManager.Writer("filesystem.csv", []string{"mount", "node", "timestamp"}, []string{"used", "free"}, &mount, &node, &timestamp, &used, &free)

	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "Filesystem",
			Reader:      ReadOnlySomeLogs(logsStore, "df"),
			Transformer: transformer.MakeDoFunc(extractFilesystemUsage),
			Writer:      filesystemUsageStore,
		},
		transformer.PipelineStage{
			Name:   "WriteFilesystemUsageCsv",
			Reader: filesystemUsageStore,
			Writer: csvStore,
		},
	}
}

func parseFilesystemString(usageString string) (int64, error) {
	used, err := strconv.Atoi(usageString)
	if err != nil {
		log.Println("Error parsing integer: %v", err)
		return 0, nil
	}
	return int64(used), nil
}

func extractFilesystemUsage(record *store.Record, outputChan chan *store.Record) {
	var logKey LogKey
	lex.DecodeOrDie(record.Key, &logKey)

	lines := strings.Split(string(record.Value), "\n")
	if len(lines) < 1 {
		log.Println("Not enough lines")
		return
	}
	for _, line := range lines[1:] {
		if len(line) <= 1 {
			continue
		}
		words := strings.Fields(line)
		if len(words) < 6 {
			log.Println("Not enough words")
			continue
		}
		used, err := parseFilesystemString(words[2])
		if err != nil {
			continue
		}
		free, err := parseFilesystemString(words[3])
		if err != nil {
			continue
		}
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(strings.Trim(words[5], "\000"), logKey.Node, logKey.Timestamp),
			Value: lex.EncodeOrDie(used, free),
		}
	}
}
