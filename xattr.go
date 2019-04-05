package hdfs

import (
	"os"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/gogo/protobuf/proto"
)

// ListUserAttrs - returns a list of all extended attributes from user namespace,
// returned is a map of key and values.
func (c *Client) ListUserAttrs(name string) (map[string]string, error) {
	req := &hdfs.ListXAttrsRequestProto{Src: proto.String(name)}
	resp := &hdfs.ListXAttrsResponseProto{}

	err := c.namenode.Execute("listXAttrs", req, resp)
	if err != nil {
		return nil, err
	}

	if resp.GetXAttrs() == nil {
		return nil, os.ErrNotExist
	}

	m := make(map[string]string)
	for _, xattr := range resp.GetXAttrs() {
		m[xattr.GetName()] = string(xattr.GetValue())

	}
	return m, nil
}

// RemoveUserAttr - removes a given user namespace extended attribute
func (c *Client) RemoveUserAttr(name, key string) error {
	req := &hdfs.RemoveXAttrRequestProto{
		Src: proto.String(name),
		XAttr: &hdfs.XAttrProto{
			Namespace: hdfs.XAttrProto_USER.Enum(),
			Name:      proto.String(key),
		},
	}
	resp := &hdfs.RemoveXAttrResponseProto{}

	return c.namenode.Execute("removeXAttr", req, resp)
}

// SetUserAttr - sets a extended attributed user namespace key,
// with the specified value.
func (c *Client) SetUserAttr(name, key, value string) error {
	var flag uint32 = 2
	req := &hdfs.SetXAttrRequestProto{
		Src: proto.String(name),
		XAttr: &hdfs.XAttrProto{
			Namespace: hdfs.XAttrProto_USER.Enum(),
			Name:      proto.String(key),
			Value:     []byte(value),
		},
		Flag: &flag,
	}
	resp := &hdfs.SetXAttrResponseProto{}

	return c.namenode.Execute("setXAttr", req, resp)
}

// GetUserAttrs - gets all the user namespace extended attributes, returned
// value is in key value map string of string. Optionally you can provide
// specific keys to get values from.
func (c *Client) GetUserAttrs(name string, keys ...string) (map[string]string, error) {

	req := &hdfs.GetXAttrsRequestProto{Src: proto.String(name)}
	for _, key := range keys {
		req.XAttrs = append(req.XAttrs, &hdfs.XAttrProto{
			Namespace: hdfs.XAttrProto_USER.Enum(),
			Name:      proto.String(key),
		})
	}
	resp := &hdfs.GetXAttrsResponseProto{}

	err := c.namenode.Execute("getXAttrs", req, resp)
	if err != nil {
		return nil, err
	}

	if resp.GetXAttrs() == nil {
		return nil, os.ErrNotExist
	}

	m := make(map[string]string)
	for _, xattr := range resp.GetXAttrs() {
		m[xattr.GetName()] = string(xattr.GetValue())

	}
	return m, nil
}
