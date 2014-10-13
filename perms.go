package hdfs

import (
	"code.google.com/p/goprotobuf/proto"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	"os"
)

// Chmod changes the mode of the named file to mode.
func (c *Client) Chmod(name string, perm os.FileMode) error {
	req := &hdfs.SetPermissionRequestProto{
		Src:        proto.String(name),
		Permission: &hdfs.FsPermissionProto{Perm: proto.Uint32(uint32(perm))},
	}
	resp := &hdfs.SetPermissionResponseProto{}

	err := c.namenode.Execute("setPermission", req, resp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return err
	}

	return nil
}

// Chown changes the user and group of the file. Unlike os.Chown, this takes
// a string username and group (since that's what HDFS uses.)
//
// If an empty string is passed for user or group, that field will not be
// changed remotely.
func (c *Client) Chown(name string, user, group string) error {
	req := &hdfs.SetOwnerRequestProto{
		Src:       proto.String(name),
		Username:  proto.String(user),
		Groupname: proto.String(group),
	}
	resp := &hdfs.SetOwnerResponseProto{}

	err := c.namenode.Execute("setOwner", req, resp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return err
	}

	return nil
}
