package main

import (
	"fmt"
	"log"
	"os"
	"strings"

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

To alter the default locations from which configurations are loaded, 
the following environment variables may be used:

  - HADOOP_CONF_DIR     hadoop configuration directory. Defaults to $HADOOP_HOME/conf 
  - HADOOP_KRB_CONF     kerberos configuration file. Default: %s
  - HADOOP_CCACHE       credential cache to use. Defaults: to "/tmp/krb5cc_{user_uid}"
  - HADOOP_KEYTAB       if set, the specified keytab is used and the credential cache is ignored.
`, os.Args[0], hdfs.KrbDefaultCfgPath)

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

	cachedClient *hdfs.Client
	status       = 0
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

// getClient returns a HDFS client to the namenode or namenods provided.
// if an empty string is provided, the env var HADOOP_NAMENODE is looked up.
// one or multiple namenodes may be specified in a comma separated list: "<namenode1>:<port>,<namenode2>:<port>,..."
func getClient(namenodes string) (*hdfs.Client, error) {
	if cachedClient != nil {
		return cachedClient, nil
	}

	hadoopCfg := hdfs.LoadHadoopConf("")

	options := hdfs.ClientOptions{}
	if namenodes != "" {
		options.Addresses = strings.Split(namenodes, ",")
	} else {
		options.Addresses = getNameNodes(hadoopCfg)
	}

	options.KerberosClient = hdfs.GetKrbClientIfRequired(hadoopCfg)
	options.ServicePrincipalName = hdfs.GetServiceName()

	c, err := hdfs.NewClient(options)
	if err != nil {
		return nil, err
	}

	cachedClient = c

	return cachedClient, nil
}

// getNameNodes checks the HADOOP_NAMENODE or the passed configuration for the namenode servers
func getNameNodes(conf hdfs.HadoopConf) []string {

	if env := os.Getenv("HADOOP_NAMENODE"); env != "" {
		return strings.Split(env, ",")
	}

	nn, err := conf.Namenodes()

	if err != nil {
		log.Fatal(err)
	}

	return nn
}
