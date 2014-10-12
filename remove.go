package hdfs

import (
	"code.google.com/p/goprotobuf/proto"
	"errors"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
)

// Remove removes the named file or directory.
func (c *Client) Remove(name string) error {
	_, err := c.getFileInfo(name)
	if err != nil {
		return err
	}

	req := &hdfs.DeleteRequestProto{
		Src:       proto.String(name),
		Recursive: proto.Bool(true),
	}
	resp := &hdfs.DeleteResponseProto{}

	err = c.namenode.Execute("delete", req, resp)
	if err != nil {
		return err
	} else if resp.Result == nil {
		return errors.New("Unexpected empty response to 'delete' rpc call")
	}

	return nil
}
