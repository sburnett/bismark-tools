package main

import (
	"flag"
	"fmt"

	"github.com/sburnett/bismark-tools/experiments-manager-logs-processing/experiments"
	"github.com/sburnett/cube"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func pipelineIndex() transformer.Pipeline {
	flagset := flag.NewFlagSet("index", flag.ExitOnError)
	tarballsPath := flagset.String("tarballs_path", "/data/users/sburnett/synced-from-dp4/bismark-experiments-manager", "Read tarballs from this directory.")
	dbRoot := flagset.String("logs_leveldb_root", "/data/users/sburnett/bismark-experiments-manager-logs-leveldb", "Write leveldbs in this directory.")
	flagset.Parse(flag.Args()[1:])
	return experiments.IndexTarballsPipeline(*tarballsPath, store.NewLevelDbManager(*dbRoot))
}

func pipelineDisjointPackages() transformer.Pipeline {
	flagset := flag.NewFlagSet("disjoint", flag.ExitOnError)
	dbRoot := flagset.String("logs_leveldb_root", "/data/users/sburnett/bismark-experiments-manager-logs-leveldb", "Write leveldbs in this directory.")
	csvOutput := flagset.String("csv_output", "/dev/null", "Write disjoint packages statistics in CSV format to this file.")
	flagset.Parse(flag.Args()[1:])
	return experiments.DisjointPackagesPipeline(store.NewLevelDbManager(*dbRoot), store.NewCsvFileManager(*csvOutput))
}

func main() {
	pipelineFuncs := map[string]transformer.PipelineThunk{
		"index":    pipelineIndex,
		"disjoint": pipelineDisjointPackages,
	}
	name, pipeline := transformer.ParsePipelineChoice(pipelineFuncs)

	go cube.Run(fmt.Sprintf("bismark_experiments_manager_logs_pipeline_%s", name))

	transformer.RunPipeline(pipeline)
}
