package health

import (
	"io/ioutil"
	"log"
	"os"
)

const LogDuringTests bool = false

func init() {
	if !LogDuringTests {
		DisableLogging()
	}
}

func EnableLogging() {
	log.SetOutput(os.Stderr)
}

func DisableLogging() {
	log.SetOutput(ioutil.Discard)
}
