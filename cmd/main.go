package main

import (
	"os"
	"taxee/pkg/assert"
)

func main() {
	cliArgs := os.Args

	assert.True(len(cliArgs) > 0, "Need to specify command")

	switch cliArgs[0] {
	case "fetch":
		// fetch txs
	case "parse":
		// parse
	case "parse-server":
		// long running server
	}
}
