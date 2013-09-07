package common

import (
    "bytes"

	"github.com/sburnett/lexicographic-tuples"
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
