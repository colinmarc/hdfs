package hdfs

import (
	"os"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

// TODO:  getTypesQuotaUsage
// https://github.com/apache/hadoop/blob/daafc8a0b849ffdf851c6a618684656925f1df76/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/QuotaUsage.java#L348C20-L348C38

// QuotaUsage represents quota usage about a file or directory in
// HDFS. It's provided directly by the namenode, and has no unix filesystem
// analogue.
type QuotaUsage struct {
	name       string
	quotaUsage *hdfs.QuotaUsageProto
}

// GetQuotaUsage returns a QuotaUsage representing the named file or
// directory. The quota usage contains information about the entire tree rooted
// in the named file
func (c *Client) GetQuotaUsage(name string) (*QuotaUsage, error) {
	qu, err := c.getQuotaUsage(name)
	if err != nil {
		err = &os.PathError{
			Op:   "quota usage",
			Path: name,
			Err:  interpretException(err)}
	}

	return qu, err
}

func (c *Client) getQuotaUsage(name string) (*QuotaUsage, error) {
	req := &hdfs.GetQuotaUsageRequestProto{Path: proto.String(name)}
	resp := &hdfs.GetQuotaUsageResponseProto{}

	err := c.namenode.Execute("getQuotaUsage", req, resp)
	if err != nil {
		return nil, err
	}

	return &QuotaUsage{name, resp.GetUsage()}, nil
}

// FileAndDirectoryCount returns the total file count of the named path, including any subdirectories.
func (qu *QuotaUsage) FileAndDirectoryCount() int64 {
	return int64(qu.quotaUsage.GetFileAndDirectoryCount())
}

// NameQuota returns the HDFS configured "name quota" for the named path. The
// name quota is a hard limit on the number of directories and files inside a
// directory; see http://goo.gl/sOSJmJ for more information.
func (qu *QuotaUsage) Quota() int64 {
	return int64(qu.quotaUsage.GetQuota())
}

// SpaceQuota returns the HDFS configured "space quota" for the named path.
// The space quota is a hard limit on the number of bytes used by files in the tree rooted at that directory.
// see http://goo.gl/sOSJmJ for more information.
func (qu *QuotaUsage) SpaceQuota() int64 {
	return int64(qu.quotaUsage.GetSpaceQuota())
}

// SpaceConsumed returns the actual space consumed for the named path in HDFS.
func (qu *QuotaUsage) SpaceConsumed() int64 {
	return int64(qu.quotaUsage.GetSpaceConsumed())
}
