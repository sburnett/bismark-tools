package stats

import (
	"time"

	"github.com/dustin/go-humanize"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func SummarizePipeline(levelDbManager store.Manager, csvManager store.Manager) transformer.Pipeline {
	statsStore := levelDbManager.Reader("stats")
	statsWithHourStore := levelDbManager.ReadingDeleter("stats-with-hour")
	statsWithDayStore := levelDbManager.ReadingDeleter("stats-with-day")
	statsWithReceivedTimestampStore := levelDbManager.ReadingDeleter("stats-with-received-timestamp")
	interarrivalTimesStore := levelDbManager.ReadingDeleter("interarrival-times")
	sizeSummaryStore := levelDbManager.ReadingWriter("size-summary")
	sizeSummaryByHourStore := levelDbManager.ReadingWriter("size-summary-by-hour")
	sizeSummaryByDayStore := levelDbManager.ReadingWriter("size-summary-by-day")
	interarrivalTimesSummaryStore := levelDbManager.ReadingWriter("interarrival-times-summary")
	sizePerDayStore := levelDbManager.ReadingWriter("sizes-by-day")

	sizeSummaryWriter := makeSummaryCsvWriter(csvManager, "size-summary.csv")
	sizeSummaryByHourWriter := makeSummaryByTimestampCsvWriter(csvManager, "size-summary-by-hour.csv")
	sizeSummaryByDayWriter := makeSummaryByTimestampCsvWriter(csvManager, "size-summary-by-day.csv")
	interarrivalTimesSummaryWriter := makeSummaryCsvWriter(csvManager, "interarrival-times-summary.csv")
	sizesPerDayWriter := csvManager.Writer("sizes-per-day.csv", []string{"experiment", "node", "timestamp"}, []string{"count"}, new(string), new(string), new(int64), new(int64))

	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "SummarizeSizes",
			Reader:      statsStore,
			Transformer: transformer.TransformFunc(summarizeSizes),
			Writer:      sizeSummaryStore,
		},
		transformer.PipelineStage{
			Name:        "RekeyStatsByHour",
			Reader:      statsStore,
			Transformer: transformer.MakeMapFunc(rekeyStatsByHour),
			Writer:      store.NewTruncatingWriter(statsWithHourStore),
		},
		transformer.PipelineStage{
			Name:        "SummarizeSizesByHour",
			Reader:      statsWithHourStore,
			Transformer: transformer.TransformFunc(summarizeSizesByTimestamp),
			Writer:      sizeSummaryByHourStore,
		},
		transformer.PipelineStage{
			Name:        "RekeyStatsByDay",
			Reader:      statsStore,
			Transformer: transformer.MakeMapFunc(rekeyStatsByDay),
			Writer:      store.NewTruncatingWriter(statsWithDayStore),
		},
		transformer.PipelineStage{
			Name:        "SummarizeSizesByDay",
			Reader:      statsWithDayStore,
			Transformer: transformer.TransformFunc(summarizeSizesByTimestamp),
			Writer:      sizeSummaryByDayStore,
		},
		transformer.PipelineStage{
			Name:        "RekeyStatsByReceivedTimestamp",
			Reader:      statsStore,
			Transformer: transformer.MakeMapFunc(rekeyStatsByReceviedTimestamp),
			Writer:      store.NewTruncatingWriter(statsWithReceivedTimestampStore),
		},
		transformer.PipelineStage{
			Name:        "ComputeInterarrivalTimes",
			Reader:      statsWithReceivedTimestampStore,
			Transformer: transformer.TransformFunc(computeInterarrivalTimes),
			Writer:      store.NewTruncatingWriter(interarrivalTimesStore),
		},
		transformer.PipelineStage{
			Name:        "SummarizeInterarrival",
			Reader:      interarrivalTimesStore,
			Transformer: transformer.TransformFunc(summarizeInterarrivalTimes),
			Writer:      interarrivalTimesSummaryStore,
		},
		transformer.PipelineStage{
			Name:        "SummarizeSizesPerDay",
			Reader:      statsStore,
			Transformer: transformer.TransformFunc(summarizeSizesPerDay),
			Writer:      sizePerDayStore,
		},
		transformer.PipelineStage{
			Name:        "AggregateExperimentsPerDay",
			Reader:      sizePerDayStore,
			Transformer: transformer.TransformFunc(aggregateSizesPerDay),
			Writer:      sizePerDayStore,
		},
		transformer.PipelineStage{
			Name:   "WriteSizesSummary",
			Reader: sizeSummaryStore,
			Writer: sizeSummaryWriter,
		},
		transformer.PipelineStage{
			Name:   "WriteSizesSummaryByHour",
			Reader: sizeSummaryByHourStore,
			Writer: sizeSummaryByHourWriter,
		},
		transformer.PipelineStage{
			Name:   "WriteSizesSummaryByDay",
			Reader: sizeSummaryByDayStore,
			Writer: sizeSummaryByDayWriter,
		},
		transformer.PipelineStage{
			Name:   "WriteInterarrivalTimesSummary",
			Reader: interarrivalTimesSummaryStore,
			Writer: interarrivalTimesSummaryWriter,
		},
		transformer.PipelineStage{
			Name:   "WriteSizePerDaySummary",
			Reader: sizePerDayStore,
			Writer: sizesPerDayWriter,
		},
	}
}

func summarizeSizes(inputChan, outputChan chan *store.Record) {
	quantileComputer := NewQuantileSample(21)

	var experiment, node string
	grouper := transformer.GroupRecords(inputChan, &experiment, &node)
	for grouper.NextGroup() {
		for grouper.NextRecord() {
			record := grouper.Read()
			var statsValue StatsValue
			lex.DecodeOrDie(record.Value, &statsValue)
			quantileComputer.Append(statsValue.Size)
		}

		count := int64(quantileComputer.Count())
		quantiles := quantileComputer.Quantiles()
		statistics := []interface{}{count}
		for _, q := range quantiles {
			statistics = append(statistics, q)
		}
		quantileComputer.Reset()

		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(experiment, node),
			Value: lex.EncodeOrDie(statistics...),
		}
	}
}

func makeSummaryCsvWriter(manager store.Manager, name string) store.Writer {
	keyNames := []string{
		"experiment",
		"node",
	}
	valueNames := []string{
		"count",
	}
	for i := 0; i <= 100; i += 5 {
		valueNames = append(valueNames, humanize.Ordinal(i))
	}
	arguments := []interface{}{
		name,
		keyNames,
		valueNames,
		new(string), // experiment
		new(string), // node
		new(int64),  // count
	}
	for i := 0; i <= 100; i += 5 {
		arguments = append(arguments, new(int64))
	}
	return manager.Writer(arguments...)
}

func truncateTimestampToHour(timestampSeconds int64) int64 {
	timestamp := time.Unix(timestampSeconds, 0)
	return time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), 0, 0, 0, timestamp.Location()).Unix()
}

func rekeyStatsByHour(record *store.Record) *store.Record {
	var statsKey StatsKey
	lex.DecodeOrDie(record.Key, &statsKey)
	var statsValue StatsValue
	lex.DecodeOrDie(record.Value, &statsValue)

	hour := truncateTimestampToHour(statsValue.ReceivedTimestamp)

	return &store.Record{
		Key:   lex.EncodeOrDie(statsKey.Experiment, statsKey.Node, hour, statsKey.Filename),
		Value: record.Value,
	}
}

func truncateTimestampToDay(timestampSeconds int64) int64 {
	timestamp := time.Unix(timestampSeconds, 0)
	return time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, timestamp.Location()).Unix()
}

func rekeyStatsByDay(record *store.Record) *store.Record {
	var statsKey StatsKey
	lex.DecodeOrDie(record.Key, &statsKey)
	var statsValue StatsValue
	lex.DecodeOrDie(record.Value, &statsValue)

	day := truncateTimestampToDay(statsValue.ReceivedTimestamp)

	return &store.Record{
		Key:   lex.EncodeOrDie(statsKey.Experiment, statsKey.Node, day, statsKey.Filename),
		Value: record.Value,
	}
}

func summarizeSizesByTimestamp(inputChan, outputChan chan *store.Record) {
	quantileComputer := NewQuantileSample(101)
	quantilesToKeep := []int{0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100}

	var experiment, node string
	var timestamp int64
	grouper := transformer.GroupRecords(inputChan, &experiment, &node, &timestamp)
	for grouper.NextGroup() {
		for grouper.NextRecord() {
			record := grouper.Read()
			var statsValue StatsValue
			lex.DecodeOrDie(record.Value, &statsValue)
			quantileComputer.Append(statsValue.Size)
		}

		count := int64(quantileComputer.Count())
		quantiles := quantileComputer.Quantiles()
		quantileComputer.Reset()

		statistics := []interface{}{count}
		for idx := range quantilesToKeep {
			statistics = append(statistics, quantiles[idx])
		}

		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(experiment, node, timestamp),
			Value: lex.EncodeOrDie(statistics...),
		}
	}
}

func makeSummaryByTimestampCsvWriter(manager store.Manager, name string) store.Writer {
	keyNames := []string{
		"experiment",
		"node",
		"timestamp",
	}
	valueNames := []string{
		"count",
	}
	for _, i := range []int{0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100} {
		valueNames = append(valueNames, humanize.Ordinal(i))
	}
	arguments := []interface{}{
		name,
		keyNames,
		valueNames,
		new(string), // experiment
		new(string), // node
		new(int64),  // timestamp
		new(int64),  // count
	}
	for _ = range []int{0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100} {
		arguments = append(arguments, new(int64))
	}
	return manager.Writer(arguments...)
}

func rekeyStatsByReceviedTimestamp(record *store.Record) *store.Record {
	var statsKey StatsKey
	lex.DecodeOrDie(record.Key, &statsKey)
	var statsValue StatsValue
	lex.DecodeOrDie(record.Value, &statsValue)

	return &store.Record{
		Key: lex.EncodeOrDie(statsKey.Experiment, statsKey.Node, statsValue.ReceivedTimestamp, statsKey.Filename),
	}
}

func computeInterarrivalTimes(inputChan, outputChan chan *store.Record) {
	var experiment, node string
	grouper := transformer.GroupRecords(inputChan, &experiment, &node)
	for grouper.NextGroup() {
		var lastTimestamp int64
		for grouper.NextRecord() {
			record := grouper.Read()

			var timestamp int64
			var filename string
			lex.DecodeOrDie(record.Key, &timestamp, &filename)

			if lastTimestamp > 0 {
				interarrivalTime := timestamp - lastTimestamp
				outputChan <- &store.Record{
					Key: lex.EncodeOrDie(experiment, node, interarrivalTime, filename),
				}
			}

			lastTimestamp = timestamp
		}
	}
}

func summarizeInterarrivalTimes(inputChan, outputChan chan *store.Record) {
	quantileComputer := NewQuantileSample(21)

	var experiment, node string
	grouper := transformer.GroupRecords(inputChan, &experiment, &node)
	for grouper.NextGroup() {
		for grouper.NextRecord() {
			record := grouper.Read()
			var timestamp int64
			lex.DecodeOrDie(record.Key, &timestamp)
			quantileComputer.Append(timestamp)
		}

		count := int64(quantileComputer.Count())
		quantiles := quantileComputer.Quantiles()
		statistics := []interface{}{count}
		for _, q := range quantiles {
			statistics = append(statistics, q)
		}
		quantileComputer.Reset()

		outputChan <- &store.Record{
			Key:   lex.EncodeOrDie(experiment, node),
			Value: lex.EncodeOrDie(statistics...),
		}
	}
}

func summarizeSizesPerDay(inputChan, outputChan chan *store.Record) {
	var experiment, node string
	grouper := transformer.GroupRecords(inputChan, &experiment, &node)
	for grouper.NextGroup() {
		sizePerDay := make(map[int64]int64)
		for grouper.NextRecord() {
			record := grouper.Read()
			var statsValue StatsValue
			lex.DecodeOrDie(record.Value, &statsValue)
			roundedTimestamp := truncateTimestampToDay(statsValue.ReceivedTimestamp)
			sizePerDay[roundedTimestamp] += statsValue.Size
		}

		for timestamp, size := range sizePerDay {
			outputChan <- &store.Record{
				Key:   lex.EncodeOrDie(experiment, node, timestamp),
				Value: lex.EncodeOrDie(size),
			}
		}
	}
}

func aggregateSizesPerDay(inputChan, outputChan chan *store.Record) {
	allExperimentsCounter := make(map[string]int64)
	for record := range inputChan {
		var experiment, node string
		var timestamp int64
		lex.DecodeOrDie(record.Key, &experiment, &node, &timestamp)
		var size int64
		lex.DecodeOrDie(record.Value, &size)
		if experiment == "all" {
			continue
		}
		allExperimentsCounter[string(lex.EncodeOrDie("all", node, timestamp))] += size
	}
	for key, value := range allExperimentsCounter {
		outputChan <- &store.Record{
			Key:   []byte(key),
			Value: lex.EncodeOrDie(value),
		}
	}
}
