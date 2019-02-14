package main

import (
	"./minerlib"
	"fmt"
	"os"
	"strconv"
)

//TESTING CLIENT RPC MAIN//

func main() {
	var configfile string
	args := os.Args[1:]

	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "Usage: go run miner.go [configfile] [externalIPAddressWithoutPort] [delaySecondsUntilConnect]")
	} else {
		configfile = args[0]
		externalIpAddr := args[1]
		delaySecondsUntilConnect := args[2]
		seconds, err := strconv.Atoi(delaySecondsUntilConnect)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}

		miner, err := minerlib.Initialize(externalIpAddr, configfile, seconds)
		miner.Start()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}

	}

}
