package health

import (
	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func runFilesystemUsagePipeline(logs map[string]string) {
	levelDbManager := store.NewSliceManager()
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

	csvManager := store.NewCsvStdoutManager()

	transformer.RunPipeline(FilesystemUsagePipeline(levelDbManager, csvManager))
	csvManager.PrintToStdout("filesystem.csv")
}

func ExampleFilesystemUsage_simple() {
	contents := `Filesystem           1K-blocks      Used Available Use% Mounted on
/dev/root                 4480      4480         0 100% /rom
tmpfs                    63444       516     62928   1% /tmp
tmpfs                      512         0       512   0% /dev
/dev/mtdblock4           10368       600      9768   6% /overlay
mini_fo:/overlay          4480      4480         0 100% /`

	records := map[string]string{
		string(lex.EncodeOrDie(&LogKey{"df", "node", 61})): contents,
	}
	runFilesystemUsagePipeline(records)

	// Output:
	//
	// mount,node,timestamp,used,free
	// /,node,61,4480,0
	// /dev,node,61,0,512
	// /overlay,node,61,600,9768
	// /rom,node,61,4480,0
	// /tmp,node,61,516,62928
}

func ExampleFilesystemUsage_ignoreOtherTypes() {
	contents := `Filesystem           1K-blocks      Used Available Use% Mounted on
/dev/root                 4480      4480         0 100% /rom
tmpfs                    63444       516     62928   1% /tmp
tmpfs                      512         0       512   0% /dev
/dev/mtdblock4           10368       600      9768   6% /overlay
mini_fo:/overlay          4480      4480         0 100% /`

	records := map[string]string{
		string(lex.EncodeOrDie(&LogKey{"other", "node", 0})): contents,
	}
	runFilesystemUsagePipeline(records)

	// Output:
	//
	// mount,node,timestamp,used,free
}
