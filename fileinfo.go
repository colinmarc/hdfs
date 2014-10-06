package hdfs

import (
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"os"
	"time"
)

// FileInfo implements os.FileInfo, and provides information about a file or
// directory in HDFS.
type FileInfo struct {
	name string
	status *hdfs.HdfsFileStatusProto
}

func (fi *FileInfo) Name() string {
	return fi.name // not sure why this doesn't come back
}

func (fi *FileInfo) Size() int64 {
	return int64(fi.status.GetLength())
}

func (fi *FileInfo) Mode() os.FileMode {
	return os.FileMode(fi.status.GetPermission().GetPerm())
}

func (fi *FileInfo) ModTime() time.Time {
	return time.Unix(int64(fi.status.GetModificationTime()) / 1000, 0)
}

func (fi *FileInfo) IsDir() bool {
	return fi.status.GetFileType() == hdfs.HdfsFileStatusProto_IS_DIR
}

func (fi *FileInfo) Sys() interface{} {
	return nil
}
