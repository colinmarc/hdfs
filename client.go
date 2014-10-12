package hdfs

import (
	"code.google.com/p/goprotobuf/proto"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"strings"
)

// A Client represents a connection to an HDFS cluster
type Client struct {
	namenode *rpc.NamenodeConnection
	defaults *hdfs.FsServerDefaultsProto
}

// New returns a connected Client, or an error if it can't connect
func New(address string) (*Client, error) {
	currentUser, err := user.Current()
	if err != nil {
		return nil, err
	}

	return NewForUser(address, currentUser.Username)
}

func NewForUser(address string, user string) (*Client, error) {
	namenode, err := rpc.NewNamenodeConnection(address, user)
	if err != nil {
		return nil, err
	}

	client := &Client{namenode: namenode}

	err = client.fetchDefaults()
	if err != nil {
		return nil, err
	}

	return client, nil
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

	local, err := os.Open(dst)
	if err != nil {
		return err
	}

	_, err = io.Copy(local, remote)
	return err
}

// CreateEmptyFile creates a empty file named by filename, with the permissions
// 0644. If it already exists, os.ErrExist will be returned.
func (c *Client) CreateEmptyFile(filename string) error {
	_, err := c.getFileInfo(filename)
	if err == nil {
		return os.ErrExist
	} else if err != os.ErrNotExist {
		return err
	}

	createReq := &hdfs.CreateRequestProto{
		Src:          proto.String(filename),
		Masked:       &hdfs.FsPermissionProto{Perm: proto.Uint32(uint32(0644))},
		ClientName:   proto.String(rpc.ClientName),
		CreateFlag:   proto.Uint32(1),
		CreateParent: proto.Bool(false),
		Replication:  c.defaults.Replication,
		BlockSize:    c.defaults.BlockSize,
	}
	createResp := &hdfs.CreateResponseProto{}

	err = c.namenode.Execute("create", createReq, createResp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok && nnErr.Code == 1 {
			parts := strings.Split(filename, "/")
			parent := strings.Join(parts[:len(parts)-1], "/")
			if _, statErr := c.getFileInfo(parent); statErr == os.ErrNotExist {
				return statErr
			}
		}

		return err
	}

	completeReq := &hdfs.CompleteRequestProto{
		Src:        proto.String(filename),
		ClientName: proto.String(rpc.ClientName),
	}
	completeResp := &hdfs.CompleteResponseProto{}

	err = c.namenode.Execute("complete", completeReq, completeResp)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) fetchDefaults() error {
	req := &hdfs.GetServerDefaultsRequestProto{}
	resp := &hdfs.GetServerDefaultsResponseProto{}

	err := c.namenode.Execute("getServerDefaults", req, resp)
	if err != nil {
		return err
	}

	c.defaults = resp.GetServerDefaults()
	return nil
}
