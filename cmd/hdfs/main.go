package main

import (
	"code.google.com/p/getopt"
	"fmt"
	"os"
	"strings"
)

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
`, os.Args[0])

	lsOpts = getopt.New()
	lsl    = lsOpts.Bool('l')
	lsa    = lsOpts.Bool('a')

	rmOpts = getopt.New()
	rmr    = rmOpts.Bool('r')

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
		status = rm(rmOpts.Args(), *rmr)
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
	case "complete":
		completions := complete(argv)
		if completions != nil {
			fmt.Println(strings.Join(completions, " "))
		}
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

func fileError(name string, err error) error {
	return fmt.Errorf("%s: %s", name, err)
}

func fatal(msg ...interface{}) {
	fmt.Fprintln(os.Stderr, msg...)
	os.Exit(1)
}

func fatalWithUsage(msg ...interface{}) {
	msg = append(msg, "\n"+usage)
	fatal(msg...)
}
