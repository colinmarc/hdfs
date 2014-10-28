package main

import (
	"fmt"
	"os"
	"strings"
)

func chown(args []string, recursive bool) int {
	if len(args) < 2 {
		printHelp()
	}

	parts := strings.SplitN(args[0], ":", 2)
	owner := ""
	group := ""

	if len(parts) == 0 {
		fatal("invalid owner string:", args[0])
	} else if len(parts) == 1 {
		owner = parts[0]
	} else {
		owner = parts[0]
		group = parts[1]
	}

	expanded, client, err := getClientAndExpandedPaths(args[1:])
	if err != nil {
		fatal(err)
	}

	status := 0
	visit := func(p string, fi os.FileInfo, err error) {
		if err == nil {
			err = client.Chown(p, owner, group)
		}

		if err != nil {
			fmt.Fprintln(os.Stderr, err)
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
