package stats

import (
	"archive/tar"
	"compress/gzip"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

const MicrosecondsPerSecond = 1e6

var currentTar *expvar.String
var tarBytesRead, tarsFailed, tarsIndexed, tarsSkipped, statsFailed, statsIndexed *expvar.Int
var timestampActiveSkipped, timestampBismarkExperimentsManagerSkipped, timestampBismarkUpdaterSkipped, timestampHealthSkipped, timestampMacAnalyzerSkipped, timestampPassiveSkipped, timestampPassiveFrequentSkipped, timestampOtherSkipped *expvar.Int

func init() {
	currentTar = expvar.NewString("CurrentTar")
	tarBytesRead = expvar.NewInt("TarBytesRead")
	tarsFailed = expvar.NewInt("TarsFailed")
	tarsIndexed = expvar.NewInt("TarsIndexed")
	tarsSkipped = expvar.NewInt("TarsSkipped")
	statsFailed = expvar.NewInt("StatsFailed")
	statsIndexed = expvar.NewInt("StatsIndexed")

	timestampActiveSkipped = expvar.NewInt("TimestampActiveSkipped")
	timestampBismarkExperimentsManagerSkipped = expvar.NewInt("TimestampBismarkExperimentsManagerSkipped")
	timestampBismarkUpdaterSkipped = expvar.NewInt("TimestampBismarkUpdaterSkipped")
	timestampHealthSkipped = expvar.NewInt("TimestampHealthSkipped")
	timestampMacAnalyzerSkipped = expvar.NewInt("TimestampMacAnalyzerSkipped")
	timestampPassiveSkipped = expvar.NewInt("TimestampPassiveSkipped")
	timestampPassiveFrequentSkipped = expvar.NewInt("TimestampPassiveFrequentSkipped")
	timestampOtherSkipped = expvar.NewInt("TimestampOtherSkipped")
}

func ExtractStatsPipeline(tarballsPath string, levelDbManager store.Manager) transformer.Pipeline {
	tarballsPattern := filepath.Join(tarballsPath, "*", "*", "*", "*.tar.gz")
	tarnamesStore := levelDbManager.ReadingWriter("tarnames")
	tarnamesIndexedStore := levelDbManager.ReadingWriter("tarnames-indexed")
	statsStore := levelDbManager.Writer("stats")
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:   "ScanTarballs",
			Reader: store.NewGlobReader(tarballsPattern),
			Writer: tarnamesStore,
		},
		transformer.PipelineStage{
			Name:        "ExtractStatsFromTarballs",
			Reader:      store.NewDemuxingReader(tarnamesStore, tarnamesIndexedStore),
			Transformer: transformer.MakeMultipleOutputsGroupDoFunc(ExtractStatsFromTarballs, 2),
			Writer:      store.NewMuxingWriter(statsStore, tarnamesIndexedStore),
		},
	}
}

func parseStatsFromHeader(header *tar.Header) (*StatsKey, *StatsValue, error) {
	dirname, basename := filepath.Split(header.Name)
	dirPieces := strings.Split(dirname, "_")
	if len(dirPieces) != 4 {
		return nil, nil, fmt.Errorf("Directory in tarball must have format 'EXPERIMENT_NODE_YYYYMMDD_HHMMSS': %s", header.Name)
	}
	experiment := dirPieces[0]
	nodeId := dirPieces[1]
	creationTimestamp := int64(-1)
	switch experiment {
	case "active":
		basenamePieces := strings.Split(basename, "_")
		if len(basenamePieces) != 2 {
			timestampActiveSkipped.Add(1)
			break
		}
		timestampString := strings.TrimSuffix(basenamePieces[1], ".xml")
		timestamp, err := strconv.Atoi(timestampString)
		if err != nil {
			timestampActiveSkipped.Add(1)
			break
		}
		creationTimestamp = int64(timestamp)
	case "bismark-experiments-manager", "bismark-updater":
		dirPieces := strings.Split(basename, "_")
		if len(dirPieces) != 3 {
			if experiment == "bismark-experiments-manager" {
				timestampBismarkExperimentsManagerSkipped.Add(1)
			} else {
				timestampBismarkUpdaterSkipped.Add(1)
			}
			break
		}
		dateString := dirPieces[1]
		timeString := strings.TrimSuffix(dirPieces[2], ".gz")
		timestamp, err := time.Parse("2006-01-02 15-04-05", dateString+" "+timeString)
		if err != nil {
			if experiment == "bismark-experiments-manager" {
				timestampBismarkExperimentsManagerSkipped.Add(1)
			} else {
				timestampBismarkUpdaterSkipped.Add(1)
			}
			break
		}
		creationTimestamp = int64(timestamp.Unix())
	case "health":
		dirPieces := strings.Split(basename, "_")
		if len(dirPieces) != 4 {
			timestampHealthSkipped.Add(1)
			break
		}
		dateString := dirPieces[2]
		timeString := strings.TrimSuffix(dirPieces[3], ".tar.gz")
		timestamp, err := time.Parse("2006-01-02 15-04-05", dateString+" "+timeString)
		if err != nil {
			timestampHealthSkipped.Add(1)
			break
		}
		creationTimestamp = int64(timestamp.Unix())
	case "mac-analyzer":
		basenamePieces := strings.Split(basename, "-")
		if len(basenamePieces) < 3 {
			timestampMacAnalyzerSkipped.Add(1)
			break
		}
		timestamp, err := strconv.Atoi(basenamePieces[1])
		if err != nil {
			timestampMacAnalyzerSkipped.Add(1)
			break
		}
		creationTimestamp = int64(timestamp) / MicrosecondsPerSecond
	case "passive":
		basenamePieces := strings.Split(basename, "-")
		if len(basenamePieces) != 3 {
			timestampPassiveSkipped.Add(1)
			break
		}
		baseTimestampMicroseconds, err := strconv.Atoi(basenamePieces[1])
		if err != nil {
			timestampPassiveSkipped.Add(1)
			break
		}
		baseTimestamp := baseTimestampMicroseconds / MicrosecondsPerSecond
		sequenceNumber, err := strconv.Atoi(strings.TrimSuffix(basenamePieces[2], ".gz"))
		if err != nil {
			timestampPassiveSkipped.Add(1)
			break
		}
		creationTimestamp = int64(baseTimestamp + 30*sequenceNumber)
	case "passive-frequent":
		basenamePieces := strings.Split(basename, "-")
		if len(basenamePieces) != 3 {
			timestampPassiveSkipped.Add(1)
			break
		}
		baseTimestampMicroseconds, err := strconv.Atoi(basenamePieces[1])
		if err != nil {
			timestampPassiveSkipped.Add(1)
			break
		}
		baseTimestamp := baseTimestampMicroseconds / MicrosecondsPerSecond
		sequenceNumber, err := strconv.Atoi(basenamePieces[2])
		if err != nil {
			timestampPassiveSkipped.Add(1)
			break
		}
		creationTimestamp = int64(baseTimestamp + 5*sequenceNumber)
	default:
		creationTimestamp = -1
	}
	logKey := StatsKey{
		Experiment: experiment,
		Node:       nodeId,
		Filename:   basename,
	}
	logValue := StatsValue{
		ReceivedTimestamp: header.ModTime.Unix(),
		CreationTimestamp: creationTimestamp,
		Size:              header.Size,
	}
	return &logKey, &logValue, nil
}

func extractStatsFromTarball(tarPath string, statsChan chan *store.Record) bool {
	currentTar.Set(tarPath)
	handle, err := os.Open(tarPath)
	if err != nil {
		log.Printf("Error reading %s: %s\n", tarPath, err)
		tarsFailed.Add(1)
		return false
	}
	defer handle.Close()
	fileinfo, err := handle.Stat()
	if err != nil {
		log.Printf("Error stating %s: %s\n", tarPath, err)
		tarsFailed.Add(1)
		return false
	}
	tarBytesRead.Add(fileinfo.Size())
	unzippedHandle, err := gzip.NewReader(handle)
	if err != nil {
		log.Printf("Error unzipping tarball %s: %s\n", tarPath, err)
		tarsFailed.Add(1)
		return false
	}
	parentReader := tar.NewReader(unzippedHandle)
	for {
		parentHeader, err := parentReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			tarsFailed.Add(1)
			log.Printf("Error indexing %v: %v", tarPath, err)
			break
		}
		if parentHeader.Typeflag != tar.TypeReg && parentHeader.Typeflag != tar.TypeRegA {
			continue
		}
		statsKey, statsValue, err := parseStatsFromHeader(parentHeader)
		if err != nil {
			statsFailed.Add(1)
			continue
		}
		statsChan <- &store.Record{
			Key:   lex.EncodeOrDie(statsKey),
			Value: lex.EncodeOrDie(statsValue),
		}
		statsIndexed.Add(1)
	}
	tarsIndexed.Add(1)
	return true
}

func ExtractStatsFromTarballs(inputRecords []*store.Record, outputChans ...chan *store.Record) {
	if len(inputRecords) != 1 {
		tarsSkipped.Add(1)
		return
	}
	if inputRecords[0].DatabaseIndex != 0 {
		return
	}

	logsChan := outputChans[0]
	tarnamesChan := outputChans[1]

	var tarPath string
	lex.DecodeOrDie(inputRecords[0].Key, &tarPath)
	if extractStatsFromTarball(tarPath, logsChan) {
		tarnamesChan <- &store.Record{
			Key: lex.EncodeOrDie(tarPath),
		}
	}
}
