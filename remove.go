package hdfs

import (
	"errors"
	"os"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/golang/protobuf/proto"
)

// Remove removes the named file or (empty) directory.
func (c *ClientImpl) Remove(name string) error {
	return delete(c, name, false)
}

// RemoveAll removes path and any children it contains. It removes everything it
// can but returns the first error it encounters. If the path does not exist,
// RemoveAll returns nil (no error).
func (c *ClientImpl) RemoveAll(name string) error {
	err := delete(c, name, true)
	if os.IsNotExist(err) {
		return nil
	}

	return err
}

func delete(c *ClientImpl, name string, recursive bool) error {
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
