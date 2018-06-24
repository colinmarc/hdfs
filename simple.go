package hdfs

import (
	"io"
	"io/ioutil"
	"os"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
)

// A Client represents a connection to an HDFS cluster
type SimpleClient struct {
	namenode *rpc.NamenodeConnection
	defaults *hdfs.FsServerDefaultsProto
}

// SimpleClientOptions represents the configurable options for a client.
type SimpleClientOptions struct {
	NameServiceID string
	Addresses     []string
	Namenode      *rpc.NamenodeConnection
	User          string
}

var _ IClient = &SimpleClient{}

// NewSimpleClient returns a connected SimpleClient for the given options, or an error if
// the client could not be created.
func NewSimpleClient(options SimpleClientOptions) (*SimpleClient, error) {
	var err error

	if options.User == "" {
		options.User, err = Username()
		if err != nil {
			return nil, err
		}
	}

	if options.Addresses == nil || len(options.Addresses) == 0 {
		if options.NameServiceID == "" {
			options.NameServiceID = getDefaultNSIDFromConf()
		}
		if options.NameServiceID == "" {
			options.Addresses, err = getNameNodeFromConf()
			if err != nil {
				return nil, err
			}
		} else {
			options.Addresses, err = getAddressesByNameServiceIDFromConf(options.NameServiceID)
			if err != nil {
				return nil, err
			}
		}
	}

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

	return &SimpleClient{namenode: options.Namenode}, nil
}

// New returns a connected Client, or an error if it can't connect. The user
// will be the user the code is running under. If address is an empty string
// it will try and get the namenode address from the hadoop configuration
// files.
func NewSimpleClientForAddress(address string) (*SimpleClient, error) {
	options := SimpleClientOptions{}

	if address != "" {
		options.Addresses = []string{address}
	}

	return NewSimpleClient(options)
}

// getDefaultNSIDFromConf returns default nameservice id from the system Hadoop configuration.
func getDefaultNSIDFromConf() string {
	hadoopConf := LoadHadoopConf("")
	return hadoopConf.DefaultNSID()
}

// getNameNodeFromConf returns namenodes from the system Hadoop configuration.
func getNameNodeFromConf() ([]string, error) {
	hadoopConf := LoadHadoopConf("")

	namenodes, nnErr := hadoopConf.Namenodes()
	if nnErr != nil {
		return nil, nnErr
	}
	return namenodes, nil
}

// getAddressesByNameServiceIDFromConf returns addresss of HA namenode from the system Hadoop configuration.
func getAddressesByNameServiceIDFromConf(name string) ([]string, error) {
	hadoopConf := LoadHadoopConf("")

	addresses, nnErr := hadoopConf.AddressesByNameServiceID(name)
	if nnErr != nil {
		return nil, nnErr
	}
	return addresses, nil
}

// NewForUser returns a connected Client with the user specified, or an error if
// it can't connect.
//
// Deprecated: Use NewClient with SimpleClientOptions instead.
func NewSimpleClientForUser(address string, user string) (*SimpleClient, error) {
	return NewSimpleClient(SimpleClientOptions{
		Addresses: []string{address},
		User:      user,
	})
}

// NewForConnection returns Client with the specified, underlying rpc.NamenodeConnection.
// You can use rpc.WrapNamenodeConnection to wrap your own net.Conn.
//
// Deprecated: Use NewClient with SimpleClientOptions instead.
func NewSimpleClientForConnection(namenode *rpc.NamenodeConnection) *SimpleClient {
	client, _ := NewSimpleClient(SimpleClientOptions{Namenode: namenode})
	return client
}

// ReadFile reads the file named by filename and returns the contents.
func (c *SimpleClient) ReadFile(filename string) ([]byte, error) {
	f, err := c.Open(filename)
	if err != nil {
		return nil, err
	}

	defer f.Close()
	return ioutil.ReadAll(f)
}

// CopyToLocal copies the HDFS file specified by src to the local file at dst.
// If dst already exists, it will be overwritten.
func (c *SimpleClient) CopyToLocal(src string, dst string) error {
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
func (c *SimpleClient) CopyToRemote(src string, dst string) error {
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

func (c *SimpleClient) fetchDefaults() (*hdfs.FsServerDefaultsProto, error) {
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

// Close terminates all underlying socket connections to remote server.
func (c *SimpleClient) Close() error {
	return c.namenode.Close()
}
