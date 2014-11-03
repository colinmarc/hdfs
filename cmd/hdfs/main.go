package main

import (
	"code.google.com/p/getopt"
	"errors"
	"fmt"
	"github.com/colinmarc/hdfs"
	"os"
)

// TODO: put, du, df, tree, test, trash

var (
	usage = fmt.Sprintf(`Usage: %s COMMAND
The flags available are a subset of the POSIX ones, but should behave similarly.

Valid commands:
  ls [-la] [FILE]...
  rm [-r] FILE...
  mv [-fT] SOURCE... DEST
  mkdir [-p] FILE...
  touch [-amc] FILE...
  chmod [-R] OCTAL-MODE FILE...
  chown [-R] OWNER[:GROUP] FILE...
  cat SOURCE...
  head [-n LINES | -c BYTES] SOURCE...
  tail [-n LINES | -c BYTES] SOURCE...
  get SOURCE [DEST]
  getmerge SOURCE DEST
`, os.Args[0])

	lsOpts = getopt.New()
	lsl    = lsOpts.Bool('l')
	lsa    = lsOpts.Bool('a')

	rmOpts = getopt.New()
	rmr    = rmOpts.Bool('r')
	rmf    = rmOpts.Bool('f')

	mvOpts = getopt.New()
	mvf    = mvOpts.Bool('f')
	mvT    = mvOpts.Bool('T')

	mkdirOpts = getopt.New()
	mkdirp    = mkdirOpts.Bool('p')

	touchOpts = getopt.New()
	touchc    = touchOpts.Bool('c')

	chmodOpts = getopt.New()
	chmodR    = chmodOpts.Bool('R')

	chownOpts = getopt.New()
	chownR    = chownOpts.Bool('R')

	headTailOpts = getopt.New()
	headtailn    = headTailOpts.Int64('n', -1)
	headtailc    = headTailOpts.Int64('c', -1)

	getmergeOpts = getopt.New()
	getmergen    = getmergeOpts.Bool('n')

	cachedClient *hdfs.Client
)

func init() {
	lsOpts.SetUsage(printHelp)
	rmOpts.SetUsage(printHelp)
	mvOpts.SetUsage(printHelp)
	touchOpts.SetUsage(printHelp)
	chmodOpts.SetUsage(printHelp)
	chownOpts.SetUsage(printHelp)
}

func main() {
	if len(os.Args) < 2 {
		printHelp()
	}

	command := os.Args[1]
	argv := os.Args[1:]
	status := 0
	switch command {
	case "ls":
		lsOpts.Parse(argv)
		status = ls(lsOpts.Args(), *lsl, *lsa)
	case "rm":
		rmOpts.Parse(argv)
		status = rm(rmOpts.Args(), *rmr, *rmf)
	case "mv":
		mvOpts.Parse(argv)
		status = mv(mvOpts.Args(), *mvf, *mvT)
	case "mkdir":
		mkdirOpts.Parse(argv)
		status = mkdir(mkdirOpts.Args(), *mkdirp)
	case "touch":
		touchOpts.Parse(argv)
		status = touch(touchOpts.Args(), *touchc)
	case "chown":
		chownOpts.Parse(argv)
		status = chown(chownOpts.Args(), *chownR)
	case "chmod":
		chmodOpts.Parse(argv)
		status = chmod(chmodOpts.Args(), *chmodR)
	case "cat":
		status = cat(argv[1:])
	case "head", "tail":
		headTailOpts.Parse(argv)
		status = printSection(headTailOpts.Args(), *headtailn, *headtailc, (command == "tail"))
	case "get":
		status = get(argv[1:])
	case "getmerge":
		getmergeOpts.Parse(argv)
		status = getmerge(getmergeOpts.Args(), *getmergen)
	// it's a seeeeecret command
	case "complete":
		complete(argv)
	case "help", "-h", "-help", "--help":
		printHelp()
	default:
		fatalWithUsage("Unknown command:", command)
	}

	os.Exit(status)
}

func printHelp() {
	fmt.Fprintln(os.Stderr, usage)
	os.Exit(0)
}

func fatal(msg ...interface{}) {
	fmt.Fprintln(os.Stderr, msg...)
	os.Exit(1)
}

func fatalWithUsage(msg ...interface{}) {
	msg = append(msg, "\n"+usage)
	fatal(msg...)
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
