package health

import (
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func SummarizeHealthPipeline(levelDbManager, csvManager store.Manager) transformer.Pipeline {
	memoryStore := levelDbManager.Reader("memory")
	memoryUsageByDayStore := levelDbManager.ReadingWriter("memory-usage-by-day")
	memoryUsageByDaySummarizedStore := levelDbManager.ReadingWriter("memory-usage-by-day-summarized")
	filesystemStore := levelDbManager.Reader("filesystem")
	filesystemUsageByDayStore := levelDbManager.ReadingWriter("filesystem-usage-by-day")
	filesystemUsageByDaySummarizedStore := levelDbManager.ReadingWriter("filesystem-usage-by-day-summarized")

	var timestamp, usage int64
	var filesystem, node string
	memoryUsageSummaryCsv := csvManager.Writer("memory-usage-summary.csv", []string{"timestamp", "node"}, []string{"usage"}, &timestamp, &node, &usage)
	filesystemUsageSummaryCsv := csvManager.Writer("filesystem-usage-summary.csv", []string{"filesystem", "timestamp", "node"}, []string{"usage"}, &filesystem, &timestamp, &node, &usage)

	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "OrderMemoryUsageByTimestamp",
			Reader:      memoryStore,
			Transformer: transformer.MakeMapFunc(orderRecordsByDay),
			Writer:      memoryUsageByDayStore,
		},
		transformer.PipelineStage{
			Name:        "SummarizeMemoryUsage",
			Reader:      memoryUsageByDayStore,
			Transformer: transformer.TransformFunc(summarizeMemoryUsage),
			Writer:      memoryUsageByDaySummarizedStore,
		},
		transformer.PipelineStage{
			Name:   "WriteMemoryUsageSummaryCsv",
			Reader: memoryUsageByDaySummarizedStore,
			Writer: memoryUsageSummaryCsv,
		},
		transformer.PipelineStage{
			Name:        "OrderFilesystemUsageByTimestamp",
			Reader:      filesystemStore,
			Transformer: transformer.MakeMapFunc(orderFilesystemRecordsByDay),
			Writer:      filesystemUsageByDayStore,
		},
		transformer.PipelineStage{
			Name:        "SummarizeFilesystemUsage",
			Reader:      filesystemUsageByDayStore,
			Transformer: transformer.TransformFunc(summarizeFilesystemUsage),
			Writer:      filesystemUsageByDaySummarizedStore,
		},
		transformer.PipelineStage{
			Name:   "WriteFilesystemUsageSummaryCsv",
			Reader: filesystemUsageByDaySummarizedStore,
			Writer: filesystemUsageSummaryCsv,
		},
	}
}

func orderRecordsByDay(record *store.Record) *store.Record {
	var node string
	var timestamp int64
	lex.DecodeOrDie(record.Key, &node, &timestamp)
	dayTimestamp := truncateTimestampToDay(timestamp)

	return &store.Record{
		Key:   lex.EncodeOrDie(dayTimestamp, node, timestamp),
		Value: record.Value,
	}
}

func summarizeMemoryUsage(inputChan, outputChan chan *store.Record) {
	var timestamp int64
	grouper := transformer.GroupRecords(inputChan, &timestamp)
	for grouper.NextGroup() {
		usage := make(map[string]int64)
		for grouper.NextRecord() {
			record := grouper.Read()
			var node string
			lex.DecodeOrDie(record.Key, &node)
			var used int64
			lex.DecodeOrDie(record.Value, &used)

			if used > usage[node] {
				usage[node] = used
			}
		}
		for node, used := range usage {
			outputChan <- &store.Record{
				Key:   lex.EncodeOrDie(timestamp, node),
				Value: lex.EncodeOrDie(used),
			}
		}
	}
}

func orderFilesystemRecordsByDay(record *store.Record) *store.Record {
	var filesystem, node string
	var timestamp int64
	lex.DecodeOrDie(record.Key, &filesystem, &node, &timestamp)
	dayTimestamp := truncateTimestampToDay(timestamp)

	return &store.Record{
		Key:   lex.EncodeOrDie(filesystem, dayTimestamp, node, timestamp),
		Value: record.Value,
	}
}

func summarizeFilesystemUsage(inputChan, outputChan chan *store.Record) {
	var filesystem string
	var timestamp int64
	grouper := transformer.GroupRecords(inputChan, &filesystem, &timestamp)
	for grouper.NextGroup() {
		usage := make(map[string]int64)
		for grouper.NextRecord() {
			record := grouper.Read()
			var node string
			lex.DecodeOrDie(record.Key, &node)
			var used int64
			lex.DecodeOrDie(record.Value, &used)

			if used > usage[node] {
				usage[node] = used
			}
		}
		for node, used := range usage {
			outputChan <- &store.Record{
				Key:   lex.EncodeOrDie(filesystem, timestamp, node),
				Value: lex.EncodeOrDie(used),
			}
		}
	}
}
