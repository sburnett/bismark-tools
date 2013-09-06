package health

import (
	"bytes"
	"time"

	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer/store"
)

type LogKey struct {
	Name      string
	Node      string
	Timestamp int64
}

func (logKey *LogKey) EncodeLexicographically() ([]byte, error) {
	return lex.Encode(logKey.Name, logKey.Node, logKey.Timestamp)
}

func (logKey *LogKey) DecodeLexicographically(reader *bytes.Buffer) error {
	return lex.Read(reader, &logKey.Name, &logKey.Node, &logKey.Timestamp)
}

func ReadOnlySomeLogs(stor store.Seeker, logTypes ...string) store.Seeker {
	prefixStore := store.SliceStore{}
	prefixStore.BeginWriting()
	for _, logType := range logTypes {
		record := store.Record{
			Key: lex.EncodeOrDie(logType),
		}
		prefixStore.WriteRecord(&record)
	}
	prefixStore.EndWriting()
	return store.NewPrefixIncludingReader(stor, &prefixStore)
}

func truncateTimestampToDay(timestampSeconds int64) int64 {
	timestamp := time.Unix(timestampSeconds, 0)
	return time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, timestamp.Location()).Unix()
}
