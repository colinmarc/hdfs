package main

import (
	"fmt"
)

func formatBytes(i uint64) string {
	switch {
	case i > (1024 * 1024 * 1024 * 1024):
		return fmt.Sprintf("%#.1fT", float64(i)/1024/1024/1024/1024)
	case i > (1024 * 1024 * 1024):
		return fmt.Sprintf("%#.1fG", float64(i)/1024/1024/1024)
	case i > (1024 * 1024):
		return fmt.Sprintf("%#.1fM", float64(i)/1024/1024)
	case i > 1024:
		return fmt.Sprintf("%#.1fK", float64(i)/1024)
	default:
		return fmt.Sprintf("%dB", i)
	}
}
