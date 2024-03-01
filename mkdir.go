package hdfs

import (
	"os"
	"path"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/golang/protobuf/proto"
)

// Mkdir creates a new directory with the specified name and permission bits.
func (c *ClientImpl) Mkdir(dirname string, perm os.FileMode) error {
	return c.mkdir(dirname, perm, false)
}

// MkdirAll creates a directory for dirname, along with any necessary parents,
// and returns nil, or else returns an error. The permission bits perm are used
// for all directories that MkdirAll creates. If dirname is already a directory,
// MkdirAll does nothing and returns nil.
func (c *ClientImpl) MkdirAll(dirname string, perm os.FileMode) error {
	return c.mkdir(dirname, perm, true)
}

func (c *ClientImpl) mkdir(dirname string, perm os.FileMode, createParent bool) error {
	dirname = path.Clean(dirname)

	info, err := c.getFileInfo(dirname)
	err = interpretException(err)
	if err == nil {
		if createParent && info.IsDir() {
			return nil
		}

		return &os.PathError{"mkdir", dirname, os.ErrExist}
	} else if !os.IsNotExist(err) {
		return &os.PathError{"mkdir", dirname, err}
	}

	req := &hdfs.MkdirsRequestProto{
		Src:          proto.String(dirname),
		Masked:       &hdfs.FsPermissionProto{Perm: proto.Uint32(uint32(perm))},
		CreateParent: proto.Bool(createParent),
	}
	resp := &hdfs.MkdirsResponseProto{}

	err = c.namenode.Execute("mkdirs", req, resp)
	if err != nil {
		return &os.PathError{"mkdir", dirname, interpretException(err)}
	}

	return nil
}
