package health

import (
	"log"
	"strings"

	"github.com/sburnett/bismark-tools/common"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func PackagesPipeline(levelDbManager, csvManager, sqliteManager store.Manager) transformer.Pipeline {
	logsStore := levelDbManager.Seeker("logs")
	installedPackagesStore := levelDbManager.ReadingWriter("installed-packages")
	versionChangesStore := levelDbManager.ReadingWriter("version-changes")
	var node, packageName string
	var timestamp int64
	var version string
	csvStore := csvManager.Writer("packages.csv", []string{"node", "package", "timestamp"}, []string{"version"}, &node, &packageName, &timestamp, &version)
	sqliteStore := sqliteManager.Writer("packages", []string{"node", "package", "timestamp"}, []string{"version"}, &node, &packageName, &timestamp, &version)
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "OpkgListInstalled",
			Reader:      ReadOnlySomeLogs(logsStore, "opkg_list-installed"),
			Transformer: transformer.MakeDoFunc(extractInstalledPackages),
			Writer:      installedPackagesStore,
		},
		transformer.PipelineStage{
			Name:        "DetectVersionChanges",
			Reader:      installedPackagesStore,
			Transformer: transformer.TransformFunc(detectChangedPackageVersions),
			Writer:      versionChangesStore,
		},
		transformer.PipelineStage{
			Name:   "WriteVersionChangesSqlite",
			Reader: versionChangesStore,
			Writer: sqliteStore,
		},
		transformer.PipelineStage{
			Name:   "WriteVersionChangesCsv",
			Reader: versionChangesStore,
			Writer: csvStore,
		},
	}
}

func extractInstalledPackages(record *store.Record, outputChan chan *store.Record) {
	var logKey common.LogKey
	lex.DecodeOrDie(record.Key, &logKey)

	lines := strings.Split(string(record.Value), "\n")
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		words := strings.Split(line, " - ")
		if len(words) != 2 {
			log.Println("Invalid line format")
			continue
		}
		packageName := words[0]
		version := words[1]
		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(logKey.Node, packageName, logKey.Timestamp),
			Value: lex.EncodeOrDie(version),
		}
	}
}

func detectChangedPackageVersions(inputChan, outputChan chan *store.Record) {
	var node, packageName string
	grouper := transformer.GroupRecords(inputChan, &node, &packageName)
	for grouper.NextGroup() {
		var lastVersion string
		for grouper.NextRecord() {
			record := grouper.Read()
			var timestamp int64
			lex.DecodeOrDie(record.Key, &timestamp)
			var version string
			lex.DecodeOrDie(record.Value, &version)

			if version != lastVersion {
				outputChan <- &store.Record{
					Key:   lex.EncodeOrDie(node, packageName, timestamp),
					Value: lex.EncodeOrDie(version),
				}
			}

			lastVersion = version
		}
	}
}
