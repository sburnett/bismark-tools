package stats

import (
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func CsvPipeline(levelDbManager, csvManager store.Manager) transformer.Pipeline {
	var experiment, node, filename string
	var receivedTimestamp, creationTimestamp, size int64
	csvStore := csvManager.Writer("stats.csv", []string{"experiment", "node", "filename"}, []string{"received_timestamp", "creation_timestamp", "size"}, &experiment, &node, &filename, &receivedTimestamp, &creationTimestamp, &size)
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:   "WriteStatsCsv",
			Reader: levelDbManager.Reader("stats"),
			Writer: csvStore,
		},
	}
}
