package main

import (
	"github.com/colinmarc/hdfs/v2"
	"os"
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

	err = client.Truncate(args[1], size)
	if pe, ok := err.(*os.PathError); ok && pe.Err == hdfs.ErrTruncateAsync {
		return
	} else if err != nil {
		fatal(err)
	}
}
