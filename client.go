package hdfs

import (
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"strings"
	"time"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	ha "github.com/colinmarc/hdfs/protocol/hadoop_ha"
	"github.com/colinmarc/hdfs/rpc"
	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
	"fmt"
)

// A Client represents a connection to an HDFS cluster
type Client struct {
	namenode *rpc.NamenodeConnection
	defaults *hdfs.FsServerDefaultsProto
}

// Username returns the value of HADOOP_USER_NAME in the environment, or
// the current system user if it is not set.
func Username() (string, error) {
	username := os.Getenv("HADOOP_USER_NAME")
	if username != "" {
		return username, nil
	}
	currentUser, err := user.Current()
	if err != nil {
		return "", err
	}
	return currentUser.Username, nil
}

// New returns a connected Client, or an error if it can't connect. The user
// will be the user the code is running under.
func New(address string) (*Client, error) {
	username, err := Username()
	if err != nil {
		return nil, err
	}

	return NewForUser(address, username)
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

func NewHA(zkAddresses string, nameservice string) (*Client, error) {
	username, err := Username()
	if err != nil {
		return nil, err
	}
	return NewForUserHA(zkAddresses, nameservice, username)
}

func NewForUserHA(zkAddresses string, nameservice string, user string) (*Client, error) {
	zkConnection, _, err := zk.Connect(strings.Split(zkAddresses, ","), 5 * time.Second)
	if err != nil {
		return nil, err
	}
	defer zkConnection.Close()
	zkBreadCrumbPath := "/hadoop-ha/" + nameservice + "/ActiveBreadCrumb"
	data, _, err := zkConnection.Get(zkBreadCrumbPath)
	if err != nil {
		return nil, err
	}
	info := &ha.ActiveNodeInfo{}
	if err = proto.Unmarshal(data, info); err != nil {
		return nil, err
	}
	client, err := NewForUser(fmt.Sprint(*info.Hostname, ":", *info.Port), user)
	return client, err
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
