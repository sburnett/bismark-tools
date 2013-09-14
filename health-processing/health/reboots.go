package health

import (
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func RebootsPipeline(levelDbManager, csvManager, sqliteManager store.Manager) transformer.Pipeline {
	uptimeStore := levelDbManager.Seeker("uptime")
	rebootsStore := levelDbManager.ReadingWriter("reboots")
	var node string
	var timestamp int64
	rebootsCsvStore := csvManager.Writer("reboots.csv", []string{"node", "boot_timestamp"}, []string{}, &node, &timestamp)
	rebootsSqliteStore := sqliteManager.Writer("reboots", []string{"node", "boot_timestamp"}, []string{}, &node, &timestamp)

	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "InferReboots",
			Reader:      uptimeStore,
			Transformer: transformer.TransformFunc(inferReboots),
			Writer:      rebootsStore,
		},
		transformer.PipelineStage{
			Name:   "WriteRebootsCsv",
			Reader: rebootsStore,
			Writer: rebootsCsvStore,
		},
		transformer.PipelineStage{
			Name:   "WriteRebootsSqlite",
			Reader: rebootsStore,
			Writer: rebootsSqliteStore,
		},
	}
}

func inferReboots(inputChan, outputChan chan *store.Record) {
	var node string
	grouper := transformer.GroupRecords(inputChan, &node)
	for grouper.NextGroup() {
		lastUptime := int64(-1)
		maxReboot := int64(-1)
		for grouper.NextRecord() {
			record := grouper.Read()
			var timestamp int64
			lex.DecodeOrDie(record.Key, &timestamp)
			var uptime int64
			lex.DecodeOrDie(record.Value, &uptime)

			if lastUptime >= 0 && lastUptime > uptime {
				if maxReboot > -1 {
					outputChan <- &store.Record{
						Key: lex.EncodeOrDie(node, maxReboot),
					}
				}
				maxReboot = int64(-1)
			}
			reboot := timestamp - uptime
			if maxReboot < reboot {
				maxReboot = reboot
			}
			lastUptime = uptime
		}

		if maxReboot > -1 {
			outputChan <- &store.Record{
				Key: lex.EncodeOrDie(node, maxReboot),
			}
		}
	}
}
