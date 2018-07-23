package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/colinmarc/hdfs"
	"github.com/pborman/getopt"
)

// TODO: cp, tree, test, trash

var (
	version string
	usage   = fmt.Sprintf(`Usage: %s COMMAND
The flags available are a subset of the POSIX ones, but should behave similarly.

Valid commands:
  ls [-lah] [FILE]...
  rm [-rf] FILE...
  mv [-nT] SOURCE... DEST
  mkdir [-p] FILE...
  touch [-amc] FILE...
  chmod [-R] OCTAL-MODE FILE...
  chown [-R] OWNER[:GROUP] FILE...
  cat SOURCE...
  head [-n LINES | -c BYTES] SOURCE...
  tail [-n LINES | -c BYTES] SOURCE...
  du [-sh] FILE...
  checksum FILE...
  get SOURCE [DEST]
  getmerge SOURCE DEST
  put SOURCE DEST
  df [-h]
`, os.Args[0])

	lsOpts = getopt.New()
	lsl    = lsOpts.Bool('l')
	lsa    = lsOpts.Bool('a')
	lsh    = lsOpts.Bool('h')

	rmOpts = getopt.New()
	rmr    = rmOpts.Bool('r')
	rmf    = rmOpts.Bool('f')

	mvOpts = getopt.New()
	mvn    = mvOpts.Bool('n')
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

	duOpts = getopt.New()
	dus    = duOpts.Bool('s')
	duh    = duOpts.Bool('h')

	getmergeOpts = getopt.New()
	getmergen    = getmergeOpts.Bool('n')

	dfOpts = getopt.New()
	dfh    = dfOpts.Bool('h')

	cachedClients map[string]*hdfs.Client = make(map[string]*hdfs.Client)
	status                                = 0
)

func init() {
	lsOpts.SetUsage(printHelp)
	rmOpts.SetUsage(printHelp)
	mvOpts.SetUsage(printHelp)
	touchOpts.SetUsage(printHelp)
	chmodOpts.SetUsage(printHelp)
	chownOpts.SetUsage(printHelp)
	headTailOpts.SetUsage(printHelp)
	duOpts.SetUsage(printHelp)
	getmergeOpts.SetUsage(printHelp)
	dfOpts.SetUsage(printHelp)
}

func main() {
	if len(os.Args) < 2 {
		printHelp()
	}

	command := os.Args[1]
	argv := os.Args[1:]
	switch command {
	case "-v", "--version":
		fatal("gohdfs version", version)
	case "ls":
		lsOpts.Parse(argv)
		ls(lsOpts.Args(), *lsl, *lsa, *lsh)
	case "rm":
		rmOpts.Parse(argv)
		rm(rmOpts.Args(), *rmr, *rmf)
	case "mv":
		mvOpts.Parse(argv)
		mv(mvOpts.Args(), !*mvn, *mvT)
	case "mkdir":
		mkdirOpts.Parse(argv)
		mkdir(mkdirOpts.Args(), *mkdirp)
	case "touch":
		touchOpts.Parse(argv)
		touch(touchOpts.Args(), *touchc)
	case "chown":
		chownOpts.Parse(argv)
		chown(chownOpts.Args(), *chownR)
	case "chmod":
		chmodOpts.Parse(argv)
		chmod(chmodOpts.Args(), *chmodR)
	case "cat":
		cat(argv[1:])
	case "head", "tail":
		headTailOpts.Parse(argv)
		printSection(headTailOpts.Args(), *headtailn, *headtailc, (command == "tail"))
	case "du":
		duOpts.Parse(argv)
		du(duOpts.Args(), *dus, *duh)
	case "checksum":
		checksum(argv[1:])
	case "get":
		get(argv[1:])
	case "getmerge":
		getmergeOpts.Parse(argv)
		getmerge(getmergeOpts.Args(), *getmergen)
	case "put":
		put(argv[1:])
	case "df":
		dfOpts.Parse(argv)
		df(*dfh)
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
	if cachedClients[namenode] != nil {
		return cachedClients[namenode], nil
	}

	if namenode == "" {
		namenode = os.Getenv("HADOOP_NAMENODE")
	}

	// Ignore errors here, since we don't care if the conf doesn't exist if the
	// namenode was specified.
	conf := hdfs.LoadHadoopConf("")
	options, _ := hdfs.ClientOptionsFromConf(conf)
	if namenode != "" {
		options.Addresses = []string{namenode}
	}

	if options.Addresses == nil {
		return nil, errors.New("Couldn't find a namenode to connect to. You should specify hdfs://<namenode>:<port> in your paths. Alternatively, set HADOOP_NAMENODE or HADOOP_CONF_DIR in your environment.")
	}

	var err error
	options.User = os.Getenv("HADOOP_USER_NAME")
	if options.User == "" {
		options.User, err = hdfs.Username()
		if err != nil {
			return nil, fmt.Errorf("Couldn't determine user: %s", err)
		}
	}

	c, err := hdfs.NewClient(options)
	if err != nil {
		return nil, fmt.Errorf("Couldn't connect to namenode: %s", err)
	}

	cachedClients[namenode] = c
	return c, nil
}
