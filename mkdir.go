package hdfs

import (
	"code.google.com/p/goprotobuf/proto"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	"os"
	"strings"
)

// Mkdir creates a new directory with the specified name and permission bits.
func (c *Client) Mkdir(name string, perm os.FileMode) error {
	return c.mkdir(name, perm, false)
}

// MkdirAll creates a directory named path, along with any necessary parents,
// and returns nil, or else returns an error. The permission bits perm are used
// for all directories that MkdirAll creates. If path is already a directory,
// MkdirAll does nothing and returns nil.
func (c *Client) MkdirAll(path string, perm os.FileMode) error {
	return c.mkdir(path, perm, true)
}

func (c *Client) mkdir(path string, perm os.FileMode, createParent bool) error {
	path = strings.TrimSuffix(path, "/")

	_, err := c.getFileInfo(path)
	if err == nil {
		return os.ErrExist
	} else if err != os.ErrNotExist {
		return err
	}

	req := &hdfs.MkdirsRequestProto{
		Src:          proto.String(path),
		Masked:       &hdfs.FsPermissionProto{Perm: proto.Uint32(uint32(perm))},
		CreateParent: proto.Bool(createParent),
	}
	resp := &hdfs.MkdirsResponseProto{}

	err = c.namenode.Execute("mkdirs", req, resp)
	if err != nil {
		// Hadoop makes this unecessarily complicated
		if nnErr, ok := err.(*rpc.NamenodeError); ok && nnErr.Code == 1 {
			parts := strings.Split(path, "/")
			parent := strings.Join(parts[:len(parts)-1], "/")
			if _, statErr := c.getFileInfo(parent); statErr == os.ErrNotExist {
				return statErr
			}
		}

		return err
	}

	return nil
}
