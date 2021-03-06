package commands

import (
	"fmt"
	"io"
	"strings"
)

func fprintWithTabs(writer io.Writer, values ...interface{}) (int, error) {
	formatString := strings.Repeat("%v\t", len(values))
	return fmt.Fprintf(writer, strings.TrimRight(formatString, "\t")+"\n", values...)
}
