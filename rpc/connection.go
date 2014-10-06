package rpc

import (
	"bufio"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"net"
	"sync"
	"time"
)

const connectionTimeout = 5 * time.Second

type Connection struct {
	callId  int
	user    string
	conn    net.Conn
	reqLock sync.Mutex
}

// NewConnection creates a new connection to a Namenode, and preforms an initial
// handshake.
//
// You probably want to use hdfs.New instead, which provides a higher-level
// interface.
func NewConnection(address string, user string) (*Connection, error) {
	conn, err := net.DialTimeout("tcp", address, connectionTimeout)
	if err != nil {
		return nil, err
	}

	return WrapConnection(conn, user)
}

// WrapConnection wraps an existing net.Conn to a Namenode, and preforms an
// initial handshake.
//
// You probably want to use hdfs.New instead, which provides a higher-level
// interface.
func WrapConnection(conn net.Conn, user string) (*Connection, error) {
	c := &Connection{
		user: user,
		conn: conn,
	}

	err := c.handshake()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("Error performing handshake: %s", err)
	}

	return c, nil
}

// Execute performs an rpc call. It does this by sending req over the wire and
// unmarshaling the result into resp.
func (c *Connection) Execute(method string, req proto.Message, resp proto.Message) error {
	c.reqLock.Lock()
	defer c.reqLock.Unlock()

	c.callId = (c.callId + 1) % 9
	reqBytes, err := makeRequest(c.callId, method, req)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(reqBytes)
	if err != nil {
		c.conn.Close()
		return err
	}

	err = readResponse(c.callId, bufio.NewReader(c.conn), resp)
	if err != nil {
		c.conn.Close() // TODO don't close on RPC failure
		return err
	}

	return nil
}

func (c *Connection) handshake() error {
	handshakeBytes, err := makeConnectionHandshake(c.user)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(handshakeBytes)
	if err != nil {
		return err
	}

	return nil
}
