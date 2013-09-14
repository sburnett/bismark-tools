package health

import (
	"database/sql"
	"log"
	"strings"

	_ "github.com/mattn/go-sqlite3"
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

type writeVersionChangesSqlite string

func (filename writeVersionChangesSqlite) Do(inputChan, outputChan chan *store.Record) {
	db, err := sql.Open("sqlite3", string(filename))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS packages (node string, package string, timestamp integer, version string)"); err != nil {
		panic(err)
	}
	if _, err := db.Exec("DELETE FROM packages"); err != nil {
		panic(err)
	}
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	stmt, err := tx.Prepare("INSERT INTO packages (node, package, timestamp, version) VALUES (?, ?, ?, ?)")
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	for record := range inputChan {
		var node, packageName string
		var timestamp int64
		lex.DecodeOrDie(record.Key, &node, &packageName, &timestamp)
		var version string
		lex.DecodeOrDie(record.Value, &version)

		if _, err := stmt.Exec(node, packageName, timestamp, version); err != nil {
			panic(err)
		}
	}
	tx.Commit()
}
