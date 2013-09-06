package main

import (
	"flag"
	"fmt"

	"github.com/sburnett/bismark-tools/uploads-stats-processing/stats"
	"github.com/sburnett/cube"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func pipelineCsv() transformer.Pipeline {
	flagset := flag.NewFlagSet("csv", flag.ExitOnError)
	csvOutput := flagset.String("csv_output", "/dev/null", "Write upload statistics to a file in this directory.")
	dbRoot := flagset.String("uploads_leveldb_root", "/data/users/sburnett/bismark-upload-stats-leveldb", "Write leveldbs in this directory.")
	flagset.Parse(flag.Args()[1:])
	return stats.CsvPipeline(store.NewLevelDbManager(*dbRoot), store.NewCsvFileManager(*csvOutput))
}

func pipelineStats() transformer.Pipeline {
	flagset := flag.NewFlagSet("stats", flag.ExitOnError)
	dbRoot := flagset.String("uploads_leveldb_root", "/data/users/sburnett/bismark-upload-stats-leveldb", "Write leveldbs in this directory.")
	tarballsPath := flagset.String("tarballs_path", "/var/local/home/bismark-data-xfer/bismark_data_from_s3", "Read tarballs from this directory.")
	flagset.Parse(flag.Args()[1:])
	return stats.ExtractStatsPipeline(*tarballsPath, store.NewLevelDbManager(*dbRoot))
}

func pipelineSummarize() transformer.Pipeline {
	flagset := flag.NewFlagSet("csv", flag.ExitOnError)
	dbRoot := flagset.String("uploads_leveldb_root", "/data/users/sburnett/bismark-upload-stats-leveldb", "Write leveldbs in this directory.")
	csvOutput := flagset.String("csv_output", "/dev/null", "Directory where we will write statistics in CSV format.")
	flagset.Parse(flag.Args()[1:])
	return stats.SummarizePipeline(store.NewLevelDbManager(*dbRoot), store.NewCsvFileManager(*csvOutput))
}

func pipelineTimesCsv() transformer.Pipeline {
	flagset := flag.NewFlagSet("csv", flag.ExitOnError)
	dbRoot := flagset.String("uploads_leveldb_root", "/data/users/sburnett/bismark-upload-stats-leveldb", "Write leveldbs in this directory.")
	csvOutput := flagset.String("csv_output", "/dev/null", "Directory where we will write statistics in CSV format.")
	flagset.Parse(flag.Args()[1:])
	return stats.TimesCsvPipeline(store.NewLevelDbManager(*dbRoot), *csvOutput)
}

func main() {
	pipelineFuncs := map[string]transformer.PipelineThunk{
		"csv":       pipelineCsv,
		"stats":     pipelineStats,
		"summarize": pipelineSummarize,
		"timescsv":  pipelineTimesCsv,
	}
	name, pipeline := transformer.ParsePipelineChoice(pipelineFuncs)

	go cube.Run(fmt.Sprintf("bismark_uploads_stats_%s", name))

	transformer.RunPipeline(pipeline)
}
