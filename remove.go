package hdfs

import (
	"errors"
	"os"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/golang/protobuf/proto"
)

// Remove removes the named file or directory non-recursively.
func (c *Client) Remove(name string) error {
	return delete(c, name, false)
}

// RemoveAll removes the named file or directory recursively.
func (c *Client) RemoveAll(name string) error {
	return delete(c, name, true)
}

func delete(c *Client, name string, recursive bool) error {
	_, err := c.getFileInfo(name)
	if err != nil {
		return &os.PathError{"remove", name, err}
	}

	req := &hdfs.DeleteRequestProto{
		Src:       proto.String(name),
		Recursive: proto.Bool(recursive),
	}
	resp := &hdfs.DeleteResponseProto{}

	err = c.namenode.Execute("delete", req, resp)
	if err != nil {
		return &os.PathError{"remove", name, interpretException(err)}
	} else if resp.Result == nil {
		return &os.PathError{
			"remove",
			name,
			errors.New("unexpected empty response"),
		}
	}

	return nil
}
