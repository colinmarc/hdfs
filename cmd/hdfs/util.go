package main

import (
	"fmt"
	"strconv"
)

func formatBytes(i uint64) string {
	switch {
	case i >= (1024 * 1024 * 1024 * 1024):
		return fmt.Sprintf("%#.1fT", float64(i)/1024/1024/1024/1024)
	case i >= (1024 * 1024 * 1024):
		return fmt.Sprintf("%#.1fG", float64(i)/1024/1024/1024)
	case i >= (1024 * 1024):
		return fmt.Sprintf("%#.1fM", float64(i)/1024/1024)
	case i >= 1024:
		return fmt.Sprintf("%#.1fK", float64(i)/1024)
	default:
		return fmt.Sprintf("%dB", i)
	}
}

func formatSize(size uint64, humanReadable bool) string {
	if humanReadable {
		return formatBytes(size)
	} else {
		return strconv.FormatUint(size, 10)
	}
}

func joinHeaders(headers []string) string {
	result := ""
	for _, header := range headers {
		result += fmt.Sprintf("%s \t", header)
	}
	return result + "\n"
}
