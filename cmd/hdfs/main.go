package main

import (
	"code.google.com/p/getopt"
	"fmt"
	"os"
	"strings"
)

var (
	lsOpts = getopt.New()
	lsa    = lsOpts.Bool('a')
	lsl    = lsOpts.Bool('l')

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
		printHelp(0)
	default:
		fatal("Unknown command:", command, "\n"+usage)
	}

	os.Exit(0)
}

func printHelp(exit int) {
	fmt.Fprintln(os.Stderr, usage)
	os.Exit(exit)
}

func fatal(msg ...interface{}) {
	fmt.Fprintln(os.Stderr, msg...)
	os.Exit(1)
}
