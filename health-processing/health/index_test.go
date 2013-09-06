package health

import (
	"fmt"
)

func parseAndPrintLogKey(filename string) {
	parsedKey, err := parseLogKey(filename)
	if err != nil {
		panic(err)
	}
	fmt.Println(parsedKey.Name, parsedKey.Node, parsedKey.Timestamp)
}

func ExampleParseLogKey_simple() {
	parseAndPrintLogKey("health_OW0123456789AB_1970-01-01_00-01-01/log")

	// Output:
	//
	// log OW0123456789AB 61
}

func ExampleParseLogKey_extra() {
	parseAndPrintLogKey("./health_OW0123456789AB_1970-01-01_00-01-01/log")

	// Output:
	//
	// log OW0123456789AB 61
}
