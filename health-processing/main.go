package main

import (
	"flag"
	"fmt"

	"github.com/sburnett/bismark-tools/health-processing/health"
	"github.com/sburnett/cube"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func pipelineIndex() transformer.Pipeline {
	flagset := flag.NewFlagSet("index", flag.ExitOnError)
	tarballsPath := flagset.String("tarballs_path", "/data/users/sburnett/bismark-health", "Read tarballs from this directory.")
	dbRoot := flagset.String("health_leveldb_root", "/data/users/sburnett/bismark-health-leveldb", "Write leveldbs in this directory.")
	flagset.Parse(flag.Args()[1:])
	return health.IndexTarballsPipeline(*tarballsPath, store.NewLevelDbManager(*dbRoot))
}

func pipelineFilesystem() transformer.Pipeline {
	flagset := flag.NewFlagSet("filesystem", flag.ExitOnError)
	dbRoot := flagset.String("health_leveldb_root", "/data/users/sburnett/bismark-health-leveldb", "Write leveldbs in this directory.")
	csvOutput := flagset.String("csv_output", "/dev/null", "Write filesystem usage in CSV format to this file.")
	flagset.Parse(flag.Args()[1:])
	return health.FilesystemUsagePipeline(store.NewLevelDbManager(*dbRoot), store.NewCsvFileManager(*csvOutput))
}

func pipelineMemory() transformer.Pipeline {
	flagset := flag.NewFlagSet("memory", flag.ExitOnError)
	dbRoot := flagset.String("health_leveldb_root", "/data/users/sburnett/bismark-health-leveldb", "Write leveldbs in this directory.")
	csvOutput := flagset.String("csv_output", "/dev/null", "Write memory usage in CSV format to this file.")
	flagset.Parse(flag.Args()[1:])
	return health.MemoryUsagePipeline(store.NewLevelDbManager(*dbRoot), store.NewCsvFileManager(*csvOutput))
}

func pipelineUptime() transformer.Pipeline {
	flagset := flag.NewFlagSet("uptime", flag.ExitOnError)
	dbRoot := flagset.String("health_leveldb_root", "/data/users/sburnett/bismark-health-leveldb", "Write leveldbs in this directory.")
	csvOutput := flagset.String("csv_output", "/dev/null", "Write memory usage in CSV format to this file.")
	flagset.Parse(flag.Args()[1:])
	return health.UptimePipeline(store.NewLevelDbManager(*dbRoot), store.NewCsvFileManager(*csvOutput))
}

func pipelineReboots() transformer.Pipeline {
	flagset := flag.NewFlagSet("reboots", flag.ExitOnError)
	dbRoot := flagset.String("health_leveldb_root", "/data/users/sburnett/bismark-health-leveldb", "Write leveldbs in this directory.")
	csvOutput := flagset.String("csv_output", "/dev/null", "Write reboots to a CSV file in this directory.")
	flagset.Parse(flag.Args()[1:])
	return health.RebootsPipeline(store.NewLevelDbManager(*dbRoot), store.NewCsvFileManager(*csvOutput))
}

func pipelineSummarize() transformer.Pipeline {
	flagset := flag.NewFlagSet("summarize", flag.ExitOnError)
	dbRoot := flagset.String("health_leveldb_root", "/data/users/sburnett/bismark-health-leveldb", "Write leveldbs in this directory.")
	csvOutput := flagset.String("csv_output", "/dev/null", "Write reboots to a CSV file in this directory.")
	flagset.Parse(flag.Args()[1:])
	return health.SummarizeHealthPipeline(store.NewLevelDbManager(*dbRoot), store.NewCsvFileManager(*csvOutput))
}

func pipelinePackages() transformer.Pipeline {
	flagset := flag.NewFlagSet("packages", flag.ExitOnError)
	dbRoot := flagset.String("health_leveldb_root", "/data/users/sburnett/bismark-health-leveldb", "Write leveldbs in this directory.")
	csvOutput := flagset.String("csv_output", "/dev/null", "Write reboots to a CSV file in this directory.")
	sqliteFilename := flagset.String("sqlite_filename", "/dev/null", "Write to this sqlite database.")
	flagset.Parse(flag.Args()[1:])
	return health.PackagesPipeline(store.NewLevelDbManager(*dbRoot), store.NewCsvFileManager(*csvOutput), store.NewSqliteManager(*sqliteFilename))
}

func main() {
	pipelineFuncs := map[string]transformer.PipelineThunk{
		"index":      pipelineIndex,
		"filesystem": pipelineFilesystem,
		"memory":     pipelineMemory,
		"packages":   pipelinePackages,
		"reboots":    pipelineReboots,
		"uptime":     pipelineUptime,
		"summarize":  pipelineSummarize,
	}
	name, pipeline := transformer.ParsePipelineChoice(pipelineFuncs)

	go cube.Run(fmt.Sprintf("bismark_health_pipeline_%s", name))

	transformer.RunPipeline(pipeline)
}
