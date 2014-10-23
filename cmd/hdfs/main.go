package main

import (
	"code.google.com/p/getopt"
	"errors"
	"fmt"
	"github.com/colinmarc/hdfs"
	"os"
	"strings"
)

var (
	cachedClient *hdfs.Client

	lsOpts = getopt.New()
	lsa    = lsOpts.Bool('a')
	lsl    = lsOpts.Bool('l')

	knownCommands = []string{"ls", "rm", "mv"}

	usage = fmt.Sprintf(`Usage: %s COMMAND [OPTION]... [FILE]...
The flags available are a subset of the POSIX ones, but should behave similarly.

Valid commands:
	ls [-la]
`, os.Args[0])
)

func main() {
	if len(os.Args) < 2 {
		printHelp(0)
	}

	command := os.Args[1]
	switch command {
	case "ls":
		lsOpts.Parse(os.Args[1:])
		ls(lsOpts.Args(), *lsa, *lsl)
	case "complete":
		completions := complete(os.Args[2:])
		if completions != nil {
			fmt.Println(strings.Join(completions, " "))
		}
	case "help", "-h", "-help", "--help":
		printHelp(0)
	default:
		fatal("Unknown command:", command, "\n"+usage)
	}

	os.Exit(0)
}

func getClient(namenode string) (*hdfs.Client, error) {
	if cachedClient != nil {
		return cachedClient, nil
	}

	if namenode == "" {
		namenode = os.Getenv("HADOOP_NAMENODE")
	}

	if namenode == "" {
		return nil, errors.New("Couldn't find a namenode to connect to. You should specify hdfs://<namenode>:<port> in your paths, or set HADOOP_NAMENODE in your environment.")
	}

	c, err := hdfs.New(namenode)
	if err != nil {
		return nil, err
	}

	cachedClient = c
	return cachedClient, nil
}

func printHelp(exit int) {
	fmt.Fprintln(os.Stderr, usage)
	os.Exit(exit)
}

func fatal(msg ...interface{}) {
	fmt.Fprintln(os.Stderr, msg...)
	os.Exit(1)
}
