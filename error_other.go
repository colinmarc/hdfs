// +build !plan9

package hdfs

import (
	"syscall"
)

var osErrNotEmpty = syscall.ENOTEMPTY
