package main

import (
	"strconv"
)

func truncate(args []string) {
	if len(args) != 2 {
		fatalWithUsage()
	}

	size, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		fatal(err)
	}

	client, err := getClient("")
	if err != nil {
		fatal(err)
	}

	_, err = client.Truncate(args[1], size)
	if err != nil {
		fatal(err)
	}
}
