package stats

import (
	"archive/tar"
	"fmt"
	"time"
)

func parseAndPrintStats(header *tar.Header) {
	statsKey, statsValue, err := parseStatsFromHeader(header)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s,%s,%s: %d,%d,%d\n", statsKey.Experiment, statsKey.Node, statsKey.Filename, statsValue.ReceivedTimestamp, statsValue.CreationTimestamp, statsValue.Size)
}

func ExampleParseLogKey_active() {
	modTime, err := time.Parse("2006-01-02 15:04", "2013-07-02 12:21")
	if err != nil {
		panic(err)
	}
	parseAndPrintStats(&tar.Header{
		Name:    "active_OW008EF25FBBDE_20130705_190001/OW008EF25FBBDE_1373049738.xml",
		Size:    134,
		ModTime: modTime,
	})

	// Output:
	//
	// active,OW008EF25FBBDE,OW008EF25FBBDE_1373049738.xml: 1372767660,1373049738,134
}

func ExampleParseLogKey_bismarkexperimentsmanager() {
	modTime, err := time.Parse("2006-01-02 15:04", "2013-07-02 12:21")
	if err != nil {
		panic(err)
	}
	parseAndPrintStats(&tar.Header{
		Name:    "bismark-experiments-manager_OWC43DC7A3EE43_20130801_180012/OWC43DC7A3EE43_2013-08-01_17-01-06.gz",
		Size:    134,
		ModTime: modTime,
	})

	// Output:
	//
	// bismark-experiments-manager,OWC43DC7A3EE43,OWC43DC7A3EE43_2013-08-01_17-01-06.gz: 1372767660,1375376466,134
}

func ExampleParseLogKey_bismarkupdater() {
	modTime, err := time.Parse("2006-01-02 15:04", "2013-07-02 12:21")
	if err != nil {
		panic(err)
	}
	parseAndPrintStats(&tar.Header{
		Name:    "bismark-updater_OW4C60DEE6B037_20130801_180014/OW4C60DEE6B037_2013-08-01_17-01-41.gz",
		Size:    134,
		ModTime: modTime,
	})

	// Output:
	//
	// bismark-updater,OW4C60DEE6B037,OW4C60DEE6B037_2013-08-01_17-01-41.gz: 1372767660,1375376501,134
}

func ExampleParseLogKey_health() {
	modTime, err := time.Parse("2006-01-02 15:04", "2013-07-02 12:21")
	if err != nil {
		panic(err)
	}
	parseAndPrintStats(&tar.Header{
		Name:    "health_OWC43DC78EE081_20130701_230032/health_OWC43DC78EE081_2013-07-01_22-00-41.tar.gz",
		Size:    134,
		ModTime: modTime,
	})

	// Output:
	//
	// health,OWC43DC78EE081,health_OWC43DC78EE081_2013-07-01_22-00-41.tar.gz: 1372767660,1372716041,134
}

func ExampleParseLogKey_macanalyzer() {
	modTime, err := time.Parse("2006-01-02 15:04", "2013-07-02 12:21")
	if err != nil {
		panic(err)
	}
	parseAndPrintStats(&tar.Header{
		Name:    "mac-analyzer_OWC43DC78EE081_20130627_150115/OWC43DC78EE081-1370322057227889-d-34488-1.gz",
		Size:    134,
		ModTime: modTime,
	})

	// Output:
	//
	// mac-analyzer,OWC43DC78EE081,OWC43DC78EE081-1370322057227889-d-34488-1.gz: 1372767660,1370322057,134
}

func ExampleParseLogKey_passive() {
	modTime, err := time.Parse("2006-01-02 15:04", "2013-07-02 12:21")
	if err != nil {
		panic(err)
	}
	parseAndPrintStats(&tar.Header{
		Name:    "passive_OWC43DC78EE081_20130702_130128/OWC43DC78EE081-1372648974516087-3958.gz",
		Size:    134,
		ModTime: modTime,
	})

	// Output:
	//
	// passive,OWC43DC78EE081,OWC43DC78EE081-1372648974516087-3958.gz: 1372767660,1372767714,134
}

func ExampleParseLogKey_passivefrequent() {
	modTime, err := time.Parse("2006-01-02 15:04", "2013-07-02 12:21")
	if err != nil {
		panic(err)
	}
	parseAndPrintStats(&tar.Header{
		Name:    "passive-frequent_OWC43DC79DE0F7_20130729_190130/OWC43DC79DE0F7-1366128214671801-415350",
		Size:    134,
		ModTime: modTime,
	})

	// Output:
	//
	// passive-frequent,OWC43DC79DE0F7,OWC43DC79DE0F7-1366128214671801-415350: 1372767660,1368204964,134
}
