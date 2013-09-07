package health

import (
	"github.com/sburnett/bismark-tools/common"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func runUptimePipeline(logs map[string]string) {
	levelDbManager := store.NewSliceManager()
	csvManager := store.NewCsvStdoutManager()

	logsStore := levelDbManager.Writer("logs")
	logsStore.BeginWriting()
	for encodedKey, content := range logs {
		record := store.Record{
			Key:   []byte(encodedKey),
			Value: lex.EncodeOrDie(content),
		}
		logsStore.WriteRecord(&record)
	}
	logsStore.EndWriting()

	transformer.RunPipeline(UptimePipeline(levelDbManager, csvManager))

	csvManager.PrintToStdout("uptime.csv")
}

func ExampleUptime_simple() {
	contents := ` 18:01:07 up 77 days,  4:37, load average: 0.00, 0.00, 0.00
6669474.38 6573489.68`

	records := map[string]string{
		string(lex.EncodeOrDie(&common.LogKey{"uptime", "node", 0})): contents,
	}
	runUptimePipeline(records)

	// Output:
	//
	// node,timestamp,uptime
	// node,0,6669474
}

func ExampleUptime_ignoreOtherTypes() {
	contents := ` 18:01:07 up 77 days,  4:37, load average: 0.00, 0.00, 0.00
6669474.38 6573489.68`

	records := map[string]string{
		string(lex.EncodeOrDie(&common.LogKey{"other", "node", 0})): contents,
	}
	runUptimePipeline(records)

	// Output:
	//
	// node,timestamp,uptime
}
