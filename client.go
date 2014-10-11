package hdfs

import (
	"code.google.com/p/goprotobuf/proto"
	"errors"
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
}

// New returns a connected Client, or an error if it can't connect
func New(address string) (*Client, error) {
	currentUser, err := user.Current()
	if err != nil {
		return nil, err
	}

	namenode, err := rpc.NewNamenodeConnection(address, currentUser.Username)
	if err != nil {
		return nil, err
	}

	return &Client{namenode: namenode}, nil
}

// Chmod changes the mode of the named file to mode.
func (c *Client) Chmod(name string, mode os.FileMode) error {
	f, err := c.Open(name)
	if err != nil {
		return err
	}

	return f.Chmod(mode)
}

// Chown changes the numeric uid and gid of the named file.
func (c *Client) Chown(name string, uid, gid int) error {
	f, err := c.Open(name)
	if err != nil {
		return err
	}

	return f.Chown(uid, gid)
}

// Stat returns an os.FileInfo describing the named file.
func (c *Client) Stat(name string) (fi os.FileInfo, err error) {
	return c.getFileInfo(name)
}

// ReadDir reads the directory named by dirname and returns a list of sorted
// directory entries.
func (c *Client) ReadDir(dirname string) ([]os.FileInfo, error) {
	return c.getDirList(dirname, "", 0)
}

// Mkdir creates a new directory with the specified name and permission bits.
func (c *Client) Mkdir(name string, perm os.FileMode) error {
	return c.mkdir(name, perm, false)
}

// MkdirAll creates a directory named path, along with any necessary parents,
// and returns nil, or else returns an error. The permission bits perm are used
// for all directories that MkdirAll creates. If path is already a directory,
// MkdirAll does nothing and returns nil.
func (c *Client) MkdirAll(path string, perm os.FileMode) error {
	return c.mkdir(path, perm, true)
}

// Remove removes the named file or directory.
func (c *Client) Remove(name string) error {
	_, err := c.getFileInfo(name)
	if err != nil {
		return err
	}

	req := &hdfs.DeleteRequestProto{
		Src:       proto.String(name),
		Recursive: proto.Bool(true),
	}
	resp := &hdfs.DeleteResponseProto{}

	err = c.namenode.Execute("delete", req, resp)
	if err != nil {
		return err
	} else if resp.Result == nil {
		return errors.New("Unexpected empty response to 'delete' rpc call")
	}

	return nil
}

// Rename renames (moves) a file.
func (c *Client) Rename(oldpath, newpath string) error {
	_, err := c.getFileInfo(oldpath)
	if err != nil {
		return err
	}

	_, err = c.getFileInfo(newpath)
	if err == nil {
		return os.ErrExist
	} else if err != os.ErrNotExist {
		return err
	}

	req := &hdfs.Rename2RequestProto{
		Src:           proto.String(oldpath),
		Dst:           proto.String(newpath),
		OverwriteDest: proto.Bool(true),
	}
	resp := &hdfs.Rename2ResponseProto{}

	err = c.namenode.Execute("rename2", req, resp)
	if err != nil {
		return err
	}

	return nil
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

func (c *Client) getFileInfo(name string) (fi os.FileInfo, err error) {
	req := &hdfs.GetFileInfoRequestProto{Src: proto.String(name)}
	resp := &hdfs.GetFileInfoResponseProto{}

	err = c.namenode.Execute("getFileInfo", req, resp)
	if err != nil {
		return nil, err
	}

	if resp.GetFs() == nil {
		return nil, os.ErrNotExist
	}

	return newFileInfo(resp.GetFs(), name, ""), nil
}

func (c *Client) getDirList(dirname string, after string, max int) ([]os.FileInfo, error) {
	res := make([]os.FileInfo, 0)
	last := after
	for max <= 0 || len(res) < max {
		partial, remaining, err := c.getPartialDirList(dirname, last)
		if err != nil {
			return nil, err
		}

		res = append(res, partial...)
		if remaining == 0 {
			break
		}
	}

	if max > 0 && len(res) > max {
		res = res[:max]
	}

	return res, nil
}

func (c *Client) getPartialDirList(dirname string, after string) ([]os.FileInfo, int, error) {
	dirname = strings.TrimSuffix(dirname, "/")

	req := &hdfs.GetListingRequestProto{
		Src:          proto.String(dirname),
		StartAfter:   []byte(after),
		NeedLocation: proto.Bool(false),
	}
	resp := &hdfs.GetListingResponseProto{}

	err := c.namenode.Execute("getListing", req, resp)
	if err != nil {
		return nil, 0, err
	}

	list := resp.GetDirList().GetPartialListing()
	res := make([]os.FileInfo, 0, len(list))
	for _, status := range list {
		res = append(res, newFileInfo(status, "", dirname))
	}

	remaining := int(resp.GetDirList().GetRemainingEntries())
	return res, remaining, nil
}

func (c *Client) mkdir(path string, perm os.FileMode, createParent bool) error {
	path = strings.TrimSuffix(path, "/")

	req := &hdfs.MkdirsRequestProto{
		Src:          proto.String(path),
		Masked:       &hdfs.FsPermissionProto{Perm: proto.Uint32(uint32(perm))},
		CreateParent: proto.Bool(createParent),
	}
	resp := &hdfs.MkdirsResponseProto{}

	err := c.namenode.Execute("mkdirs", req, resp)
	if err != nil {
		// Hadoop makes this unecessarily complicated
		if nnErr, ok := err.(*rpc.NamenodeError); ok && nnErr.Code == 1 {
			parts := strings.Split(path, "/")
			parent := strings.Join(parts[:len(parts)-1], "/")
			if _, statErr := c.getFileInfo(parent); statErr == os.ErrNotExist {
				return statErr
			}
		}

		return err
	}

	return nil
}
