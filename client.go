package hdfs

import (
	"os"
	"os/user"
	"path/filepath"
	"time"
)

type IClient interface {
	Append(name string) (*FileWriter, error)
	Chmod(name string, perm os.FileMode) error
	Chown(name string, user, group string) error
	Chtimes(name string, atime time.Time, mtime time.Time) error
	Close() error
	CopyToLocal(src string, dst string) error
	CopyToRemote(src string, dst string) error
	Create(name string) (*FileWriter, error)
	CreateEmptyFile(name string) error
	CreateFile(name string, replication int, blockSize int64, perm os.FileMode) (*FileWriter, error)
	GetContentSummary(name string) (*ContentSummary, error)
	Mkdir(dirname string, perm os.FileMode) error
	MkdirAll(dirname string, perm os.FileMode) error
	Open(name string) (*FileReader, error)
	ReadDir(dirname string) ([]os.FileInfo, error)
	ReadFile(filename string) ([]byte, error)
	Remove(name string) error
	Rename(oldpath, newpath string) error
	Stat(name string) (os.FileInfo, error)
	StatFs() (FsInfo, error)
	Walk(root string, walkFn filepath.WalkFunc) error
}

/// Client is a proxy to IClient, for compat reason
type Client struct {
	realc IClient
}

// ClientOptions
type ClientOptions struct {
	// the conf, if missing, load default conf from external file
	Conf HadoopConf
	// the root nsid, if missing, use defaultFS from conf
	RootNameServiceID string
	// the user name, if missing, use HADOOP_USER_NAME env var
	User string
	// force connect to this addresses instead of smart choice
	Addresses []string
}

var _ IClient = Client{}

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

// NewForUser returns a connected Client with the user specified, or an error if
// it can't connect.
//
// Deprecated: Use NewClient with SimpleClientOptions instead.
func NewForUser(address string, user string) (*Client, error) {
	t, err := NewSimpleClientForUser(address, user)
	if err != nil {
		return nil, err
	} else {
		return &Client{t}, nil
	}
}

// New returns a connected Client, or an error if it can't connect. The user
// will be the user the code is running under. If address is an empty string
// it will try and get the namenode address from the hadoop configuration
// files. If address is a NameServiceID in conf file, it will connect to one
// of its HA node, If address is NameServiceID with Viewfs mount links, it
// will return a ViewfsClient to handle links
func New(maybe_addr string) (*Client, error) {
	if maybe_addr != "" {
		conf := LoadHadoopConf("")
		o := conf.CheckTypeOfNameAddressString(maybe_addr)
		switch o {
		case TNAS_SimpleAddress, TNAS_SimpleNameServiceID:
			{
				t, err := NewSimpleClientForAddress(maybe_addr)
				if err != nil {
					return nil, err
				} else {
					return &Client{t}, nil
				}
			}
		case TNAS_ViewfsNameServiceID:
			{
				t, err := NewViewfsClientForRootNSID(conf, maybe_addr)
				if err != nil {
					return nil, err
				} else {
					return &Client{t}, nil
				}
			}
		default:
			return nil, errUnresolvedNamenode
		}

	} else {
		t, err := NewViewfsClientDefault()
		if err != nil {
			return nil, err
		} else {
			return &Client{t}, nil
		}
	}
}

func NewClient(options ClientOptions) (*Client, error) {
	var err error

	if options.User == "" {
		options.User, err = Username()
		if err != nil {
			return nil, err
		}
	}

	if options.Addresses != nil && len(options.Addresses) > 0 {
		opt2 := SimpleClientOptions{
			User:      options.User,
			Addresses: options.Addresses,
		}
		t, err := NewSimpleClient(opt2)
		if err != nil {
			return nil, err
		} else {
			return &Client{t}, nil
		}
	} else {
		opt2 := ViewfsClientOptions{
			Conf:              options.Conf,
			RootNameServiceID: options.RootNameServiceID,
			User:              options.User,
		}
		t, err := NewViewfsClient(opt2)
		if err != nil {
			return nil, err
		} else {
			return &Client{t}, nil
		}

	}
}

// Client is a proxy to IClient
//====================================

func (c Client) Append(name string) (*FileWriter, error) {
	return c.realc.Append(name)
}
func (c Client) Chmod(name string, perm os.FileMode) error {
	return c.realc.Chmod(name, perm)
}
func (c Client) Chown(name string, user, group string) error {
	return c.realc.Chown(name, user, group)
}
func (c Client) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return c.realc.Chtimes(name, atime, mtime)
}
func (c Client) Close() error {
	return c.realc.Close()
}
func (c Client) CopyToLocal(src string, dst string) error {
	return c.realc.CopyToLocal(src, dst)
}
func (c Client) CopyToRemote(src string, dst string) error {
	return c.realc.CopyToRemote(src, dst)
}
func (c Client) Create(name string) (*FileWriter, error) {
	return c.realc.Create(name)
}
func (c Client) CreateEmptyFile(name string) error {
	return c.realc.CreateEmptyFile(name)
}
func (c Client) CreateFile(name string, replication int, blockSize int64, perm os.FileMode) (*FileWriter, error) {
	return c.realc.CreateFile(name, replication, blockSize, perm)
}
func (c Client) GetContentSummary(name string) (*ContentSummary, error) {
	return c.realc.GetContentSummary(name)
}
func (c Client) Mkdir(dirname string, perm os.FileMode) error {
	return c.realc.Mkdir(dirname, perm)
}
func (c Client) MkdirAll(dirname string, perm os.FileMode) error {
	return c.realc.MkdirAll(dirname, perm)
}
func (c Client) Open(name string) (*FileReader, error) {
	return c.realc.Open(name)
}
func (c Client) ReadDir(dirname string) ([]os.FileInfo, error) {
	return c.realc.ReadDir(dirname)
}
func (c Client) ReadFile(filename string) ([]byte, error) {
	return c.realc.ReadFile(filename)
}
func (c Client) Remove(name string) error {
	return c.realc.Remove(name)
}
func (c Client) Rename(oldpath, newpath string) error {
	return c.realc.Rename(oldpath, newpath)
}
func (c Client) Stat(name string) (os.FileInfo, error) {
	return c.realc.Stat(name)
}
func (c Client) StatFs() (FsInfo, error) {
	return c.realc.StatFs()
}
func (c Client) Walk(root string, walkFn filepath.WalkFunc) error {
	return c.realc.Walk(root, walkFn)
}

//====================================
