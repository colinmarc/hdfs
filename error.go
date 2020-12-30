package hdfs

import (
	"os"
	"syscall"
)

const (
	fileNotFoundException      = "java.io.FileNotFoundException"
	permissionDeniedException  = "org.apache.hadoop.security.AccessControlException"
	pathIsNotEmptyDirException = "org.apache.hadoop.fs.PathIsNotEmptyDirectoryException"
	fileAlreadyExistsException = "org.apache.hadoop.fs.FileAlreadyExistsException"
)

// Error represents a remote java exception from an HDFS namenode or datanode.
type Error interface {
	// Method returns the RPC method that encountered an error.
	Method() string
	// Desc returns the long form of the error code (for example ERROR_CHECKSUM).
	Desc() string
	// Exception returns the java exception class name (for example
	// java.io.FileNotFoundException).
	Exception() string
	// Message returns the full error message, complete with java exception
	// traceback.
	Message() string
}

func interpretException(err error) error {
	var exception string
	if remoteErr, ok := err.(Error); ok {
		exception = remoteErr.Exception()
	}

	switch exception {
	case fileNotFoundException:
		return os.ErrNotExist
	case permissionDeniedException:
		return os.ErrPermission
	case pathIsNotEmptyDirException:
		return syscall.ENOTEMPTY
	case fileAlreadyExistsException:
		return os.ErrExist
	default:
		return err
	}
}
