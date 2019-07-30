package main

import (
	"github.com/colinmarc/hdfs/v2"
	"os"
	"time"
)

func touch(paths []string, noCreate bool, accessTime bool, modifyTime bool) {
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

		finfo, err := client.Stat(p)
		exists := !os.IsNotExist(err)
		if (err != nil && exists) || (!exists && noCreate) {
			fatal(err)
		}

		if exists {
			if accessTime {
				now := time.Now()
				atime := now
				err = client.Chtimes(p, atime, finfo.ModTime())
			}
			if modifyTime {
				now := time.Now()
				mtime := now
				hdfsFileInfo := finfo.(*hdfs.FileInfo)
				atime := hdfsFileInfo.AccessTime()
				err = client.Chtimes(p, atime, mtime)
			}
		} else {
			err = client.CreateEmptyFile(p)
		}

		if err != nil {
			fatal(err)
		}
	}
}
