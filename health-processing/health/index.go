package health

import (
	"archive/tar"
	"compress/gzip"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sburnett/bismark-tools/common"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

var currentTar *expvar.String
var tarBytesRead, tarsFailed, nestedTarsFailed, tarsIndexed, nestedTarsIndexed, tarsSkipped, logsFailed, logsIndexed *expvar.Int

func init() {
	currentTar = expvar.NewString("CurrentTar")
	tarBytesRead = expvar.NewInt("TarBytesRead")
	tarsFailed = expvar.NewInt("TarsFailed")
	nestedTarsFailed = expvar.NewInt("NestedTarsFailed")
	tarsIndexed = expvar.NewInt("TarsIndexed")
	nestedTarsIndexed = expvar.NewInt("NestedTarsIndexed")
	tarsSkipped = expvar.NewInt("TarsSkipped")
	logsFailed = expvar.NewInt("TracesFailed")
	logsIndexed = expvar.NewInt("TracesIndexed")
}

func IndexTarballsPipeline(tarballsPath string, levelDbManager store.Manager) transformer.Pipeline {
	tarballsPattern := filepath.Join(tarballsPath, "*", "*", "health_*.tar.gz")
	tarnamesStore := levelDbManager.ReadingWriter("tarnames")
	tarnamesIndexedStore := levelDbManager.ReadingWriter("tarnames-indexed")
	logsStore := levelDbManager.Writer("logs")
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:   "ScanLogTarballs",
			Reader: store.NewGlobReader(tarballsPattern),
			Writer: tarnamesStore,
		},
		transformer.PipelineStage{
			Name:        "ReadLogTarballs",
			Reader:      store.NewDemuxingReader(tarnamesStore, tarnamesIndexedStore),
			Transformer: transformer.MakeMultipleOutputsGroupDoFunc(IndexTarballs, 2),
			Writer:      store.NewMuxingWriter(logsStore, tarnamesIndexedStore),
		},
	}
}

func parseLogKey(filename string) (*common.LogKey, error) {
	dirname, basename := filepath.Split(filename)
	lastDirname := filepath.Base(dirname)

	dirPieces := strings.Split(lastDirname, "_")
	if len(dirPieces) != 4 {
		return nil, fmt.Errorf("Directory in tarball must have format 'health_NODE_YYYY-MM-DD_HH-MM-SS': %s", filename)
	}
	nodeId := dirPieces[1]
	timestamp, err := time.Parse("2006-01-02_15-04-05", dirPieces[2]+"_"+dirPieces[3])
	if err != nil {
		return nil, err
	}
	logKey := common.LogKey{
		Name:      basename,
		Node:      nodeId,
		Timestamp: timestamp.Unix(),
	}
	return &logKey, nil
}

func indexNestedTarball(reader io.Reader, logChan chan *store.Record) error {
	tarReader := tar.NewReader(reader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			continue
		}
		contents, err := ioutil.ReadAll(tarReader)
		if err != nil {
			logsFailed.Add(1)
			continue
		}
		logKey, err := parseLogKey(header.Name)
		if err != nil {
			logsFailed.Add(1)
			continue
		}
		logChan <- &store.Record{
			Key:   lex.EncodeOrDie(logKey),
			Value: contents,
		}
		logsIndexed.Add(1)
	}
	return nil
}

func indexTarball(tarPath string, logChan chan *store.Record) bool {
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
		if filepath.Ext(parentHeader.Name) != ".gz" {
			continue
		}
		parentGzipHandle, err := gzip.NewReader(parentReader)
		if err != nil {
			nestedTarsFailed.Add(1)
			log.Printf("Error gunzipping trace %s/%s: %v", tarPath, parentHeader.Name, err)
			continue
		}
		if err := indexNestedTarball(parentGzipHandle, logChan); err != nil {
			nestedTarsFailed.Add(1)
			continue
		}
		nestedTarsIndexed.Add(1)
	}
	tarsIndexed.Add(1)
	return true
}

func IndexTarballs(inputRecords []*store.Record, outputChans ...chan *store.Record) {
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
	if indexTarball(tarPath, logsChan) {
		tarnamesChan <- &store.Record{
			Key: lex.EncodeOrDie(tarPath),
		}
	}
}
