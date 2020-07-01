package rpc

import (
	"context"
	"net"

	"github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
)

// DatanodeSaslDialer is a dialer that will use the Privacy and Integrity
// data members in order to decide whether or not it needs to wrap the connection
// to the datanode for proper handling of encryption / integrity protection
// after it's been constructed it can be used as one would use net.Dialer
type DatanodeSaslDialer struct {
	Dialer    func(ctx context.Context, network, addr string) (net.Conn, error)
	Key       *EncryptionKey
	Privacy   bool
	Integrity bool
	Token     *hadoop_common.TokenProto
}

// DialContext fits the Dialer interface as per net.Dialer, if the Dialer provided
// is nil, then (&net.Dialer{}).DialContext will be used as the underlying connection
// to the datanode, afterwards if we are configured for privacy / integrity we will
// wrap the connection object here after performing the handshake so that any consumers
// of the connection don't have to care about the encryption / integrity protection
func (d *DatanodeSaslDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if d.Dialer == nil {
		d.Dialer = (&net.Dialer{}).DialContext
	}

	conn, err := d.Dialer(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	if d.Privacy || d.Integrity {
		return newDigestMD5Conn(conn, d.Token, d.Key, d.Privacy)
	}

	return conn, err
}
