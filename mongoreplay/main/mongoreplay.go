package main

import (
	"github.com/jessevdk/go-flags"
	"github.com/mongodb/mongo-tools/mongoreplay"

	"os"
)

const (
	ExitOk       = 0
	ExitError    = 1
	ExitNonFatal = 3
	// Go reserves exit code 2 for its own use
)

func main() {
	opts := mongoreplay.Options{}
	var parser = flags.NewParser(&opts, flags.Default)

	parser.Parse()
	if opts.PrintVersion() {
		os.Exit(ExitOk)
	}

	parser.AddCommand("play", "Play captured traffic against a mongodb instance", "",
		&mongoreplay.PlayCommand{GlobalOpts: &opts})
	parser.AddCommand("record", "Convert network traffic into mongodb queries", "",
		&mongoreplay.RecordCommand{GlobalOpts: &opts})
	parser.AddCommand("monitor", "Inspect live or pre-recorded mongodb traffic", "",
		&mongoreplay.MonitorCommand{GlobalOpts: &opts})

	_, err := parser.Parse()

	if err != nil {
		switch err.(type) {
		case mongoreplay.ErrPacketsDropped:
			os.Exit(ExitNonFatal)
		default:
			os.Exit(ExitError)
		}
	}
}
