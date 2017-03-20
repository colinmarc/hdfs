package hdfs

import (
	"errors"
	"os"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

var ErrTruncateAsync = errors.New("truncate currently in progress")

// Truncate truncates the file specified by name to the given size, and returns
// any error encountered. If HDFS indicates the truncate will be performed
// asynchronously, the error returned will be ErrTruncateAsync wrapped in an
// os.PathError.
func (c *Client) Truncate(name string, size int64) error {
	req := &hdfs.TruncateRequestProto{
		Src:        proto.String(name),
		NewLength:  proto.Uint64(uint64(size)),
		ClientName: proto.String(c.namenode.ClientName),
	}
	resp := &hdfs.TruncateResponseProto{}

	err := c.namenode.Execute("truncate", req, resp)
	if err != nil {
		return &os.PathError{"truncate", name, interpretException(err)}
	} else if resp.Result == nil {
		return &os.PathError{"truncate", name, errors.New("unexpected empty response")}
	} else if resp.GetResult() == false {
		return &os.PathError{"truncate", name, ErrTruncateAsync}
	}

	return nil
}
