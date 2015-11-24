package hdfs

import (
	"io"
	"io/ioutil"
	"os"
	"os/user"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	"github.com/golang/protobuf/proto"
)

// A Client represents a connection to an HDFS cluster
type Client struct {
	namenode *rpc.NamenodeConnection
	defaults *hdfs.FsServerDefaultsProto
}

// New returns a connected Client, or an error if it can't connect. The user
// will be the user the code is running under.
func New(address string) (*Client, error) {
	currentUser, err := user.Current()
	if err != nil {
		return nil, err
	}

	return NewForUser(address, currentUser.Username)
}

// NewForUser returns a connected Client with the user specified, or an error if
// it can't connect.
func NewForUser(address string, user string) (*Client, error) {
	namenode, err := rpc.NewNamenodeConnection(address, user)
	if err != nil {
		return nil, err
	}

	return &Client{namenode: namenode}, nil
}

// ReadFile reads the file named by filename and returns the contents.
func (c *Client) ReadFile(filename string) ([]byte, error) {
	f, err := c.Open(filename)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(f)
}

// CopyToLocal copies the HDFS file specified by src to the local file at dst.
// If dst already exists, it will be overwritten.
func (c *Client) CopyToLocal(src string, dst string) error {
	remote, err := c.Open(src)
	if err != nil {
		return err
	}

	local, err := os.Create(dst)
	if err != nil {
		return err
	}

	_, err = io.Copy(local, remote)
	return err
}

// CreateEmptyFile creates a empty file named by filename, with the permissions
// 0644.
func (c *Client) CreateEmptyFile(filename string) error {
	_, err := c.getFileInfo(filename)
	if err == nil {
		return &os.PathError{"create", filename, os.ErrExist}
	} else if !os.IsNotExist(err) {
		return &os.PathError{"create", filename, err}
	}

	defaults, err := c.fetchDefaults()
	if err != nil {
		return err
	}

	createReq := &hdfs.CreateRequestProto{
		Src:          proto.String(filename),
		Masked:       &hdfs.FsPermissionProto{Perm: proto.Uint32(uint32(0644))},
		ClientName:   proto.String(rpc.ClientName),
		CreateFlag:   proto.Uint32(1),
		CreateParent: proto.Bool(false),
		Replication:  proto.Uint32(defaults.GetReplication()),
		BlockSize:    proto.Uint64(defaults.GetBlockSize()),
	}
	createResp := &hdfs.CreateResponseProto{}

	err = c.namenode.Execute("create", createReq, createResp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return &os.PathError{"create", filename, err}
	}

	completeReq := &hdfs.CompleteRequestProto{
		Src:        proto.String(filename),
		ClientName: proto.String(rpc.ClientName),
	}
	completeResp := &hdfs.CompleteResponseProto{}

	err = c.namenode.Execute("complete", completeReq, completeResp)
	if err != nil {
		return &os.PathError{"create", filename, err}
	}

	return nil
}

func (c *Client) fetchDefaults() (*hdfs.FsServerDefaultsProto, error) {
	if c.defaults != nil {
		return c.defaults, nil
	}

	req := &hdfs.GetServerDefaultsRequestProto{}
	resp := &hdfs.GetServerDefaultsResponseProto{}

	err := c.namenode.Execute("getServerDefaults", req, resp)
	if err != nil {
		return nil, err
	}

	c.defaults = resp.GetServerDefaults()
	return c.defaults, nil
}
