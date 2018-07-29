package hdfs

import (
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"strings"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
)

// A Client represents a connection to an HDFS cluster
type Client struct {
	namenode *rpc.NamenodeConnection
	defaults *hdfs.FsServerDefaultsProto
	Options  ClientOptions
}

// ClientOptions represents the configurable options for a client.
type ClientOptions struct {
	// Addresses specifies the namenode(s) to connect to.
	Addresses []string
	// User specifies which HDFS user the client will act as.
	User string
	// Namenode optionally specifies an existing NamenodeConnection to wrap. This
	// is useful if you needed to create the namenode net.Conn manually for
	// whatever reason.
	Namenode *rpc.NamenodeConnection
	// UseDatanodeHostname whether clients should use datanode hostnames (instead
	// of the default, datanote IP addresses) when connecting to datanodes. This
	// may be useful for multihomed environments.
	UseDatanodeHostname bool
}

// ClientOptionsFromConf attempts to load any relevant configuration options
// from the given Hadoop configuration and create a ClientOptions struct
// suitable for creating a Client. Currently this is restricted to the namenode
// address(es), but may be expanded in the future.
func ClientOptionsFromConf(conf HadoopConf) (ClientOptions, error) {
	namenodes, err := conf.Namenodes()
	if err != nil {
		return ClientOptions{}, err
	}

	opts := ClientOptions{
		Addresses:           namenodes,
		UseDatanodeHostname: conf.UseDatanodeHostname(),
	}
	return opts, nil
}

// NewClient returns a connected Client for the given options, or an error if
// the client could not be created.
func NewClient(options ClientOptions) (*Client, error) {
	var err error

	if options.Namenode == nil {
		options.Namenode, err = rpc.NewNamenodeConnectionWithOptions(
			rpc.NamenodeConnectionOptions{
				Addresses: options.Addresses,
				User:      options.User,
			},
		)
		if err != nil {
			return nil, err
		}
	}

	return &Client{
		namenode: options.Namenode,
		Options:  options,
	}, nil
}

// New returns a connected Client, or an error if it can't connect. The user
// will be the current system user. Any relevantoptions (including the
// address(es) of the namenode(s), if an empty string is passed) will be loaded
// from the Hadoop configuration present at HADOOP_CONF_DIR.
func New(address string) (*Client, error) {
	conf := LoadHadoopConf("")
	options, err := ClientOptionsFromConf(conf)
	if err != nil {
		options = ClientOptions{}
	}

	if address != "" {
		options.Addresses = strings.Split(address, ",")
	}

	u, err := user.Current()
	if err != nil {
		return nil, err
	}

	options.User = u.Username
	return NewClient(options)
}

// NewForUser returns a connected Client with the user specified, or an error if
// it can't connect.
//
// Deprecated: Use NewClient with ClientOptions instead.
func NewForUser(address string, user string) (*Client, error) {
	return NewClient(ClientOptions{
		Addresses: []string{address},
		User:      user,
	})
}

// NewForConnection returns Client with the specified, underlying rpc.NamenodeConnection.
// You can use rpc.WrapNamenodeConnection to wrap your own net.Conn.
//
// Deprecated: Use NewClient with ClientOptions instead.
func NewForConnection(namenode *rpc.NamenodeConnection) *Client {
	client, _ := NewClient(ClientOptions{Namenode: namenode})
	return client
}

// ReadFile reads the file named by filename and returns the contents.
func (c *Client) ReadFile(filename string) ([]byte, error) {
	f, err := c.Open(filename)
	if err != nil {
		return nil, err
	}

	defer f.Close()
	return ioutil.ReadAll(f)
}

// CopyToLocal copies the HDFS file specified by src to the local file at dst.
// If dst already exists, it will be overwritten.
func (c *Client) CopyToLocal(src string, dst string) error {
	remote, err := c.Open(src)
	if err != nil {
		return err
	}
	defer remote.Close()

	local, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer local.Close()

	_, err = io.Copy(local, remote)
	return err
}

// CopyToRemote copies the local file specified by src to the HDFS file at dst.
func (c *Client) CopyToRemote(src string, dst string) error {
	local, err := os.Open(src)
	if err != nil {
		return err
	}
	defer local.Close()

	remote, err := c.Create(dst)
	if err != nil {
		return err
	}
	defer remote.Close()

	_, err = io.Copy(remote, local)
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

// RPCOptions fills out the options for RPC-specific parameters.
func (c *Client) RPCOptions() rpc.Options {
	return rpc.Options{
		ClientName:          c.namenode.ClientName(),
		UseDatanodeHostname: c.Options.UseDatanodeHostname,
	}
}

// Close terminates all underlying socket connections to remote server.
func (c *Client) Close() error {
	return c.namenode.Close()
}

// Username returns the current system user if it is not set.
//
// Deprecated: just use user.Current. Previous versions of this function would
// check the env variable HADOOP_USER_NAME; this functionality was removed.
func Username() (string, error) {
	currentUser, err := user.Current()
	if err != nil {
		return "", err
	}

	return currentUser.Username, nil
}
