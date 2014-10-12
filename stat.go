package hdfs

import (
	"code.google.com/p/goprotobuf/proto"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"os"
	"time"
)

// FileInfo implements os.FileInfo, and provides information about a file or
// directory in HDFS.
type FileInfo struct {
	name   string
	status *hdfs.HdfsFileStatusProto
}

// Stat returns an os.FileInfo describing the named file.
func (c *Client) Stat(name string) (fi os.FileInfo, err error) {
	return c.getFileInfo(name)
}

func newFileInfo(status *hdfs.HdfsFileStatusProto, name, dirname string) *FileInfo {
	fi := &FileInfo{status: status}

	var fullName string
	if string(status.GetPath()) != "" {
		fullName = string(status.GetPath())
	} else {
		fullName = name
	}

	if dirname != "" {
		fullName = dirname + "/" + fullName
	}

	fi.name = fullName
	return fi
}

func (fi *FileInfo) Name() string {
	return fi.name
}

func (fi *FileInfo) Size() int64 {
	return int64(fi.status.GetLength())
}

func (fi *FileInfo) Mode() os.FileMode {
	return os.FileMode(fi.status.GetPermission().GetPerm())
}

func (fi *FileInfo) ModTime() time.Time {
	return time.Unix(int64(fi.status.GetModificationTime())/1000, 0)
}

func (fi *FileInfo) IsDir() bool {
	return fi.status.GetFileType() == hdfs.HdfsFileStatusProto_IS_DIR
}

func (fi *FileInfo) Sys() interface{} {
	return nil
}

func (c *Client) getFileInfo(name string) (fi os.FileInfo, err error) {
	req := &hdfs.GetFileInfoRequestProto{Src: proto.String(name)}
	resp := &hdfs.GetFileInfoResponseProto{}

	err = c.namenode.Execute("getFileInfo", req, resp)
	if err != nil {
		return nil, err
	}

	if resp.GetFs() == nil {
		return nil, os.ErrNotExist
	}

	return newFileInfo(resp.GetFs(), name, ""), nil
}
