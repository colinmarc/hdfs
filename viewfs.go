package hdfs

import (
	"errors"
	"os"
	"path/filepath"
	"time"
)

var errOperationAcrossNameService = errors.New("Operation across two NameServiceID")

// ViewfClientOptions represents the configurable options for a client.
type ViewfsClientOptions struct {
	Conf              HadoopConf
	RootNameServiceID string
	User              string
}

// A Client represents connections to  multiple namenode viewfs cluster
type ViewfsClient struct {
	clients  map[string]*SimpleClient
	conf     HadoopConf
	rootnsid string
	user     string
}

var _ IClient = &ViewfsClient{}

func NewViewfsClientDefault() (*ViewfsClient, error) {
	conf := LoadHadoopConf("")
	return NewViewfsClientWithConf(conf)
}

func NewViewfsClientWithConf(conf HadoopConf) (*ViewfsClient, error) {
	options := ViewfsClientOptions{
		Conf: conf,
	}
	return NewViewfsClient(options)
}

func NewViewfsClientForRootNSIDAndUser(conf HadoopConf, nsid string, user string) (*ViewfsClient, error) {
	options := ViewfsClientOptions{
		Conf:              conf,
		RootNameServiceID: nsid,
		User:              user,
	}
	return NewViewfsClient(options)
}

func NewViewfsClientForRootNSID(conf HadoopConf, nsid string) (*ViewfsClient, error) {
	return NewViewfsClientForRootNSIDAndUser(conf, nsid, "")
}

func NewViewfsClientForUser(conf HadoopConf, user string) (*ViewfsClient, error) {
	nsid := conf.DefaultNSID()
	return NewViewfsClientForRootNSIDAndUser(conf, nsid, user)
}

func NewViewfsClient(options ViewfsClientOptions) (*ViewfsClient, error) {
	var err error

	if options.User == "" {
		options.User, err = Username()
		if err != nil {
			return nil, err
		}
	}

	c := ViewfsClient{
		clients:  make(map[string]*SimpleClient),
		conf:     options.Conf,
		rootnsid: options.RootNameServiceID,
		user:     options.User,
	}
	return &c, nil
}

// newSubClient returns a SimpleClient which connected to the specify NameServiceID
func (c *ViewfsClient) newSubClient(nsid string) (*SimpleClient, error) {
	addresses, err := c.conf.AddressesByNameServiceID(nsid)

	if err != nil {
		return nil, err
	}
	options := SimpleClientOptions{
		Addresses: addresses,
		User:      c.user,
	}

	return NewSimpleClient(options)
}

func (c *ViewfsClient) getSubClientAndNewPath(filename string) (*SimpleClient, string, error) {
	newnsid, newpath, err := c.conf.ViewfsReparseFilename(c.rootnsid, filename)
	if err != nil {
		return nil, "", err
	}
	sc, ok := c.clients[newnsid]
	if !ok || sc == nil {
		sc, err = c.newSubClient(newnsid)
		if err != nil {
			return nil, newpath, err
		}
		c.clients[newnsid] = sc
	}
	return sc, newpath, nil
}

// ReadFile reads the file named by filename and returns the contents.
func (c *ViewfsClient) ReadFile(filename string) ([]byte, error) {
	sc, newpath, err := c.getSubClientAndNewPath(filename)
	if err != nil {
		return nil, err
	} else {
		return sc.ReadFile(newpath)
	}
}

// CopyToLocal copies the HDFS file specified by src to the local file at dst.
// If dst already exists, it will be overwritten.
func (c *ViewfsClient) CopyToLocal(src string, dst string) error {
	sc, newpath, err := c.getSubClientAndNewPath(src)
	if err != nil {
		return err
	} else {
		return sc.CopyToLocal(newpath, dst)
	}
}

// CopyToRemote copies the local file specified by src to the HDFS file at dst.
func (c *ViewfsClient) CopyToRemote(src string, dst string) error {
	sc, newpath, err := c.getSubClientAndNewPath(dst)
	if err != nil {
		return err
	} else {
		return sc.CopyToLocal(src, newpath)
	}
}

func (c *ViewfsClient) Append(name string) (*FileWriter, error) {
	sc, newpath, err := c.getSubClientAndNewPath(name)
	if err != nil {
		return nil, err
	} else {
		return sc.Append(newpath)
	}
}

func (c *ViewfsClient) Chmod(name string, perm os.FileMode) error {
	sc, newpath, err := c.getSubClientAndNewPath(name)
	if err != nil {
		return err
	} else {
		return sc.Chmod(newpath, perm)
	}
}

func (c *ViewfsClient) Chown(name string, user, group string) error {
	sc, newpath, err := c.getSubClientAndNewPath(name)
	if err != nil {
		return err
	} else {
		return sc.Chown(newpath, user, group)
	}
}

func (c *ViewfsClient) Chtimes(name string, atime time.Time, mtime time.Time) error {
	sc, newpath, err := c.getSubClientAndNewPath(name)
	if err != nil {
		return err
	} else {
		return sc.Chtimes(newpath, atime, mtime)
	}
}

func (c *ViewfsClient) Create(name string) (*FileWriter, error) {
	sc, newpath, err := c.getSubClientAndNewPath(name)
	if err != nil {
		return nil, err
	} else {
		return sc.Create(newpath)
	}
}

func (c *ViewfsClient) CreateEmptyFile(name string) error {
	sc, newpath, err := c.getSubClientAndNewPath(name)
	if err != nil {
		return err
	} else {
		return sc.CreateEmptyFile(newpath)
	}
}

func (c *ViewfsClient) CreateFile(name string, replication int, blockSize int64, perm os.FileMode) (*FileWriter, error) {
	sc, newpath, err := c.getSubClientAndNewPath(name)
	if err != nil {
		return nil, err
	} else {
		return sc.CreateFile(newpath, replication, blockSize, perm)
	}
}

func (c *ViewfsClient) GetContentSummary(name string) (*ContentSummary, error) {
	sc, newpath, err := c.getSubClientAndNewPath(name)
	if err != nil {
		return nil, err
	} else {
		return sc.GetContentSummary(newpath)
	}
}

func (c *ViewfsClient) Mkdir(dirname string, perm os.FileMode) error {
	sc, newpath, err := c.getSubClientAndNewPath(dirname)
	if err != nil {
		return err
	} else {
		return sc.Mkdir(newpath, perm)
	}
}

func (c *ViewfsClient) MkdirAll(dirname string, perm os.FileMode) error {
	sc, newpath, err := c.getSubClientAndNewPath(dirname)
	if err != nil {
		return err
	} else {
		return sc.MkdirAll(newpath, perm)
	}
}

func (c *ViewfsClient) Open(name string) (*FileReader, error) {
	sc, newpath, err := c.getSubClientAndNewPath(name)
	if err != nil {
		return nil, err
	} else {
		return sc.Open(newpath)
	}
}

func (c *ViewfsClient) ReadDir(dirname string) ([]os.FileInfo, error) {
	sc, newpath, err := c.getSubClientAndNewPath(dirname)
	if err != nil {
		return nil, err
	} else {
		return sc.ReadDir(newpath)
	}
}

func (c *ViewfsClient) Remove(name string) error {
	sc, newpath, err := c.getSubClientAndNewPath(name)
	if err != nil {
		return err
	} else {
		return sc.Remove(newpath)
	}
}

func (c *ViewfsClient) Rename(oldpath, newpath string) error {
	sc_o, op2, err := c.getSubClientAndNewPath(oldpath)
	if err != nil {
		return err
	}
	sc_n, np2, err := c.getSubClientAndNewPath(newpath)
	if err != nil {
		return err
	}
	if sc_o != sc_n {
		return errOperationAcrossNameService
	} else {
		return sc_o.Rename(op2, np2)
	}
}

func (c *ViewfsClient) Stat(name string) (os.FileInfo, error) {
	sc, newpath, err := c.getSubClientAndNewPath(name)
	if err != nil {
		return nil, err
	} else {
		return sc.Stat(newpath)
	}
}

func (c *ViewfsClient) StatFsByName(name string) (FsInfo, error) {
	sc, _, err := c.getSubClientAndNewPath(name)
	if err != nil {
		return FsInfo{}, err
	} else {
		return sc.StatFs()
	}
}

func (c *ViewfsClient) StatFs() (FsInfo, error) {
	return c.StatFsByName("/")
}

// This function is not implemented correctly
// it should walk around multiple NSIDs, but it have not done it.
func (c *ViewfsClient) Walk(root string, walkFn filepath.WalkFunc) error {
	sc, newpath, err := c.getSubClientAndNewPath(root)
	if err != nil {
		return err
	} else {
		return sc.Walk(newpath, walkFn)
	}
}

// Close terminates all underlying socket connections to remote server.
func (c *ViewfsClient) Close() error {
	var err error
	for _, sc := range c.clients {
		t := sc.Close()
		if t != nil {
			err = t
		}
	}
	return err
}
