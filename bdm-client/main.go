package main

import (
	"flag"
	"fmt"
	"github.com/sburnett/bismark-tools/bdm-client/commands"
	"os"
)

func main() {
	cmds := []commands.BdmCommand{
		commands.Devices{},
		commands.Status{},
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: of %s [options] <command>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "where options are:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nAvailable commands:\n\n")
		for _, command := range cmds {
			fmt.Fprintf(os.Stderr, "%s: %s\n", command.Name(), command.Description())
		}
	}
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		return
	}

	var commandToRun commands.BdmCommand
	for _, command := range cmds {
		if flag.Arg(0) != command.Name() {
			continue
		}
		if commandToRun != nil {
			panic(fmt.Errorf("Multiple commands with the same name: %s", flag.Arg(0)))
		}
		commandToRun = command
	}

	if commandToRun == nil {
		flag.Usage()
		return
	}
	if err := commandToRun.Run(flag.Args()[1:]); err != nil {
		panic(err)
	}
}