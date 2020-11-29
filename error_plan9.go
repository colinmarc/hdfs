// +build plan9

package hdfs

import (
	"errors"
)

// plan9 does not have ENOTEMPTY
var osErrNotEmpty = errors.New(pathIsNotEmptyDirException)
