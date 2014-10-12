package hdfs

import (
	"os"
)

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
