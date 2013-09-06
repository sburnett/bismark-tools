package stats

import (
	"fmt"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
	"os"
	"path/filepath"
)

func TimesCsvPipeline(levelDbManager store.Manager, csvRoot string) transformer.Pipeline {
	writeTimesCsv := func(inputChan, outputChan chan *store.Record) {
		var currentHandle *os.File
		var currentExperiment, currentNode string
		for record := range inputChan {
			var statsKey StatsKey
			lex.DecodeOrDie(record.Key, &statsKey)
			var statsValue StatsValue
			lex.DecodeOrDie(record.Value, &statsValue)

			if currentExperiment != statsKey.Experiment || currentNode != statsKey.Node {
				if currentHandle != nil {
					currentHandle.Close()
				}
				currentExperiment = statsKey.Experiment
				currentNode = statsKey.Node

				csvName := fmt.Sprintf("%s_%s.csv", currentExperiment, currentNode)
				newHandle, err := os.Create(filepath.Join(csvRoot, csvName))
				if err != nil {
					panic(err)
				}
				currentHandle = newHandle
			}

			if _, err := fmt.Fprintf(currentHandle, "%d,%d\n", statsValue.CreationTimestamp, statsValue.ReceivedTimestamp); err != nil {
				panic(err)
			}
		}
		if currentHandle != nil {
			currentHandle.Close()
		}
	}

	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "WriteTimesCsv",
			Reader:      levelDbManager.Reader("stats"),
			Transformer: transformer.TransformFunc(writeTimesCsv),
		},
	}
}
