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
  rm [-r] [FILE]...
  mv [-fT] SOURCE... DEST
`, os.Args[0])

	lsOpts = getopt.New()
	lsl    = lsOpts.Bool('l')
	lsa    = lsOpts.Bool('a')

	rmOpts = getopt.New()
	rmr    = rmOpts.Bool('r')

	mvOpts = getopt.New()
	mvf    = mvOpts.Bool('f')
	mvT    = mvOpts.Bool('T')
)

func init() {
	lsOpts.SetUsage(printHelp)
	rmOpts.SetUsage(printHelp)
	mvOpts.SetUsage(printHelp)
}

func main() {
	if len(os.Args) < 2 {
		printHelp()
	}

	command := os.Args[1]
	status := 0
	switch command {
	case "ls":
		lsOpts.Parse(os.Args[1:])
		status = ls(lsOpts.Args(), *lsl, *lsa)
	case "rm":
		rmOpts.Parse(os.Args[1:])
		status = rm(rmOpts.Args(), *rmr)
	case "mv":
		mvOpts.Parse(os.Args[1:])
		status = mv(mvOpts.Args(), *mvf, *mvT)
	case "complete":
		var words []string
		if len(os.Args) == 3 {
			words = strings.Split(os.Args[2], " ")[1:]
		} else {
			words = make([]string, 0)
		}

		completions := complete(words)
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
