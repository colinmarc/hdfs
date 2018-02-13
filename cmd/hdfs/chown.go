package main

import (
	"fmt"
	"os"
	"strings"
)

func chown(args []string, recursive bool) {
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
		group = owner
	} else {
		owner = parts[0]
		group = parts[1]
	}

	expanded, client, err := getClientAndExpandedPaths(args[1:])
	if err != nil {
		fatal(err)
	}

	visit := func(p string, fi os.FileInfo, err error) error {
		err = client.Chown(p, owner, group)

		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			status = 1
			return err
		}
		return nil
	}

	for _, p := range expanded {
		if recursive {
			err = client.Walk(p, visit)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				status = 1
			}
		} else {
			info, err := client.Stat(p)
			if err != nil {
				fatal(err)
			}

			visit(p, info, nil)
		}
	}
}
