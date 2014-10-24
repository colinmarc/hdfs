package main

import (
	"fmt"
	"os"
	"strconv"
)

func chmod(args []string, recursive bool) int {
	if len(args) < 2 {
		printHelp()
	}

	mode, err := strconv.ParseUint(args[0], 8, 32)
	if err != nil {
		fatal("invalid octal mode:", args[0])
	}

	expanded, client, err := getClientAndExpandedPaths(args[1:])
	if err != nil {
		fatal(err)
	}

	status := 0
	visit := func(p string, fi os.FileInfo, err error) {
		if err == nil {
			err = client.Chmod(p, os.FileMode(mode))
		}

		if err != nil {
			fmt.Fprintln(os.Stderr, fileError(p, err))
			status = 1
		}
	}

	for _, p := range expanded {
		if recursive {
			walk(client, p, visit)
		} else {
			info, err := stat(client, p)
			visit(p, info, err)
		}
	}

	return status
}
