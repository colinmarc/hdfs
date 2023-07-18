package hdfs

import (
	"os"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/golang/protobuf/proto"
)

func (c *Client) RecoverLease(name string) (bool, error) {
	recoverLeaseReq := &hdfs.RecoverLeaseRequestProto{
		Src:          proto.String(name),
		ClientName:   proto.String(c.namenode.ClientName),
	}
	recoverLeaseResp := &hdfs.RecoverLeaseResponseProto{}

	err := c.namenode.Execute("recoverLease", recoverLeaseReq, recoverLeaseResp)
	if err != nil {
		return false, &os.PathError{"recoverLease", name, interpretException(err)}
	}

	return recoverLeaseResp.GetResult(), nil
}

func (c *Client) RenewLease(name string) (bool, error) {
	renewLeaseReq := &hdfs.RenewLeaseRequestProto{
		ClientName:   proto.String(c.namenode.ClientName),
	}
	renewLeaseResp := &hdfs.RecoverLeaseResponseProto{}

	err := c.namenode.Execute("renewLease", renewLeaseReq, renewLeaseResp)
	if err != nil {
		return false, &os.PathError{"renewLease", name, interpretException(err)}
	}

	return renewLeaseResp.GetResult(), nil
}

