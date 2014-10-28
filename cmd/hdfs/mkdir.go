package main

import (
	"os"
)

func mkdir(paths []string, all bool) int {
	paths, nn, err := normalizePaths(paths)
	if err != nil {
		fatal(err)
	}

	if len(paths) == 0 {
		printHelp()
	}

	client, err := getClient(nn)
	if err != nil {
		fatal(err)
	}

	for _, p := range paths {
		if hasGlob(p) {
			fatal(&os.PathError{"mkdir", p, os.ErrNotExist})
		}

		if all {
			err = client.MkdirAll(p, 0644)
		} else {
			err = client.Mkdir(p, 0644)
		}

		if err != nil && !(all && os.IsExist(err)) {
			fatal(err)
		}
	}

	return 0
}
