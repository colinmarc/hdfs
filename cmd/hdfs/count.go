package main

import (
	"fmt"
	"os"
	"text/tabwriter"
)

var (
	quotaHeaderFields   = []string{"QUOTA", "REM_QUOTA", "SPACE_QUOTA", "REM_SPACE_QUOTA"}
	summaryHeaderFields = []string{"DIR_COUNT", "FILE_COUNT", "CONTENT_SIZE", "PATHNAME"}

	allHeaderFields = append(quotaHeaderFields, summaryHeaderFields...)

	showQuotasFormat = "%v \t%v \t%v \t%v \t%v \t%v \t%v \t%s\n"
	summaryFormat    = "%v \t%v \t%v \t%s\n"

	quotaNone = "none"
	quotaInf  = "inf"
)

func count(args []string, showQuotas, humanReadable bool) {
	if len(args) == 0 {
		fatalWithUsage()
	}

	paths, client, err := getClientAndExpandedPaths(args)

	if err != nil {
		fatal(err)
	}

	if len(paths) == 0 {
		fatalWithUsage()
	}

	tw := tabwriter.NewWriter(os.Stdout, 8, 8, 0, ' ', 0)
	var headerStr string
	if showQuotas {
		headerStr = joinHeaders(allHeaderFields)

	} else {
		headerStr = joinHeaders(summaryHeaderFields)
	}
	defer tw.Flush()

	fmt.Fprintf(tw, headerStr)

	for _, p := range paths {

		var (
			size, spaceQuota, remSpaceQuota              int64
			dirCount, fileCount, nameQuota, remNameQuota int
			quotaStr                                     = quotaNone
			quotaRemStr                                  = quotaInf
			spaceQuotaStr                                = quotaNone
			spaceQuotaRemStr                             = quotaInf
		)

		cs, err := client.GetContentSummary(p)
		if err != nil {
			fmt.Fprintln(tw, err)
			status = 1
			continue
		}

		qu, err := client.GetQuotaUsage(p)
		if err != nil {
			fmt.Fprintln(tw, err)
			status = 1
			continue
		}

		size = cs.Size()

		dirCount = cs.DirectoryCount()
		fileCount = cs.FileCount()
		nameQuota = cs.NameQuota()
		spaceQuota = cs.SpaceQuota()

		remNameQuota = nameQuota - int(qu.FileAndDirectoryCount())

		remSpaceQuota = spaceQuota - qu.SpaceConsumed()

		if nameQuota > 0 {
			quotaStr = formatSize(uint64(nameQuota), humanReadable)
			quotaRemStr = formatSize(uint64(remNameQuota), humanReadable)
		}

		if spaceQuota >= 0 {
			spaceQuotaStr = formatSize(uint64(spaceQuota), humanReadable)
			spaceQuotaRemStr = formatSize(uint64(remSpaceQuota), humanReadable)
		}

		sizeStr := formatSize(uint64(size), humanReadable)

		if showQuotas {

			fmt.Fprintf(tw, showQuotasFormat,
				quotaStr,
				quotaRemStr,
				spaceQuotaStr,
				spaceQuotaRemStr,
				dirCount,
				fileCount,
				sizeStr,
				p,
			)

		} else {

			fmt.Fprintf(tw, summaryFormat,
				dirCount,
				fileCount,
				sizeStr,
				p,
			)

		}
	}
}
