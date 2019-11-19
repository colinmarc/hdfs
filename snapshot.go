package hdfs

import (
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
)

func (c *Client) AllowSnapshots(dir string) error {
	allowSnapshotReq := &hdfs.AllowSnapshotRequestProto{SnapshotRoot: &dir}
	allowSnapshotRes := &hdfs.AllowSnapshotResponseProto{}

	err := c.namenode.Execute("allowSnapshot", allowSnapshotReq, allowSnapshotRes)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}
		return err
	}

	return nil
}

func (c *Client) DisallowSnapshots(dir string) error {
	disallowSnapshotReq := &hdfs.DisallowSnapshotRequestProto{SnapshotRoot: &dir}
	disallowSnapshotRes := &hdfs.DisallowSnapshotResponseProto{}

	err := c.namenode.Execute("disallowSnapshot", disallowSnapshotReq, disallowSnapshotRes)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}
		return err
	}

	return nil
}

func (c *Client) CreateSnapshot(dir, name string) (string, error) {
	allowSnapshotReq := &hdfs.CreateSnapshotRequestProto{
		SnapshotRoot: &dir,
		SnapshotName: &name,
	}
	allowSnapshotRes := &hdfs.CreateSnapshotResponseProto{}

	err := c.namenode.Execute("createSnapshot", allowSnapshotReq, allowSnapshotRes)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}
		return "", err
	}

	return allowSnapshotRes.GetSnapshotPath(), nil
}

func (c *Client) DeleteSnapshot(dir, name string) error {
	allowSnapshotReq := &hdfs.DeleteSnapshotRequestProto{
		SnapshotRoot: &dir,
		SnapshotName: &name,
	}
	allowSnapshotRes := &hdfs.DeleteSnapshotResponseProto{}

	err := c.namenode.Execute("deleteSnapshot", allowSnapshotReq, allowSnapshotRes)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}
		return err
	}

	return nil
}
