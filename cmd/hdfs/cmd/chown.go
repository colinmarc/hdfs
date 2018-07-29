package cmd

import (
	"errors"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(chownCmd)
	chownCmd.PersistentFlags().BoolVarP(&chownRecursiveOpt, "recursive", "R", false, "operate on files and directories recursively")
}

var (
	chownRecursiveOpt bool
)

var chownCmd = &cobra.Command{
	Use:   "chown [-R] OWNER[:GROUP] FILE...",
	Short: "change HDFS file owner and group",
	RunE:  chownRun,
	DisableFlagsInUseLine: true,
}

func chownRun(cmd *cobra.Command, args []string) error {
	if len(args) < 2 {
		return cmd.Usage()
	}

	parts := strings.SplitN(args[0], ":", 2)
	owner := ""
	group := ""

	if len(parts) == 0 {
		return errors.New("invalid owner string: " + args[0])
	} else if len(parts) == 1 {
		owner = parts[0]
		group = owner
	} else {
		owner = parts[0]
		group = parts[1]
	}

	expanded, client, err := getClientAndExpandedPaths(args[1:])
	if err != nil {
		return err
	}

	visit := func(p string, fi os.FileInfo, err error) error {
		err = client.Chown(p, owner, group)

		if err != nil {
			return err
		}
		return nil
	}

	for _, p := range expanded {
		if chownRecursiveOpt {
			err = client.Walk(p, visit)
			if err != nil {
				return err
			}
		} else {
			info, err := client.Stat(p)
			if err != nil {
				return err
			}

			visit(p, info, nil)
		}
	}
	return nil
}
