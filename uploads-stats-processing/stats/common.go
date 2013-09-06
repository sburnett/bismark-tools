package stats

import (
	"bytes"

	"github.com/sburnett/lexicographic-tuples"
)

type StatsKey struct {
	Experiment string
	Node       string
	Filename   string
}

func (statsKey *StatsKey) EncodeLexicographically() ([]byte, error) {
	return lex.Encode(statsKey.Experiment, statsKey.Node, statsKey.Filename)
}

func (statsKey *StatsKey) DecodeLexicographically(reader *bytes.Buffer) error {
	return lex.Read(reader, &statsKey.Experiment, &statsKey.Node, &statsKey.Filename)
}

type StatsValue struct {
	ReceivedTimestamp int64
	CreationTimestamp int64
	Size              int64
}

func (statsValue *StatsValue) EncodeLexicographically() ([]byte, error) {
	return lex.Encode(statsValue.ReceivedTimestamp, statsValue.CreationTimestamp, statsValue.Size)
}

func (statsValue *StatsValue) DecodeLexicographically(reader *bytes.Buffer) error {
	return lex.Read(reader, &statsValue.ReceivedTimestamp, &statsValue.CreationTimestamp, &statsValue.Size)
}
