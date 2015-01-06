package main

import (
	"os"
)

func mkdir(paths []string, all bool) {
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

		var mode = 0755 | os.ModeDir
		if all {
			err = client.MkdirAll(p, mode)
		} else {
			err = client.Mkdir(p, mode)
		}

		if err != nil && !(all && os.IsExist(err)) {
			fatal(err)
		}
	}
}
