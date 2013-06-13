package commands

import (
	"fmt"
	"io"
	"strings"
	"time"
)

func fprintWithTabs(writer io.Writer, values ...interface{}) (int, error) {
	formatString := strings.Repeat("%v\t", len(values))
	return fmt.Fprintf(writer, strings.TrimRight(formatString, "\t")+"\n", values...)
}

func secondsToDurationString(seconds float64) string {
	return (time.Second * time.Duration(seconds)).String()
}
