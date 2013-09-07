package experiments

import (
	"github.com/sburnett/bismark-tools/common"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
	"strings"
)

func DisjointPackagesPipeline(levelDbManager, csvManager store.Manager) transformer.Pipeline {
	logsStore := levelDbManager.Seeker("logs")
	disjointPackagesStore := levelDbManager.ReadingWriter("disjoint-packages")
	var filename, node string
	var timestamp int64
	csvStore := csvManager.Writer("not-disjoint.csv", []string{"filename", "node", "timestamp"}, []string{}, &filename, &node, &timestamp)
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "DisjointPackages",
			Reader:      logsStore,
			Transformer: transformer.MakeDoFunc(detectDisjointPackagesError),
			Writer:      disjointPackagesStore,
		},
		transformer.PipelineStage{
			Name:   "WriteDisjointPackagesCsv",
			Reader: disjointPackagesStore,
			Writer: csvStore,
		},
	}
}

func detectDisjointPackagesError(record *store.Record, outputChan chan *store.Record) {
	var logKey common.LogKey
	lex.DecodeOrDie(record.Key, &logKey)
	lines := strings.Split(string(record.Value), "\n")
	for _, line := range lines {
		if line == "Managed and unmanaged repositories must be disjoint!" {
			outputChan <- record
		}
	}
}
