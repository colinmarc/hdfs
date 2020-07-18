package transfer

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hadoop "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/v2/internal/sasl"
)

func getTestDigest() *digestMD5Handshake {
	return &digestMD5Handshake{
		passwd:   "secret",
		authID:   []byte("chris"),
		hostname: "elwood.innosoft.com",
		service:  "imap",
	}
}

func TestMD5DigestResponse(t *testing.T) {
	dgst := getTestDigest()

	origGenCnonce := genCnonce
	genCnonce = func() (string, error) {
		return "OA6MHXh6VqTrRk", nil
	}
	defer func() {
		genCnonce = origGenCnonce
	}()

	// example pulled from page 19 of RFC 2831
	challenge := `realm="elwood.innosoft.com", nonce="OA6MG9tEQGm2hh", qop="auth", algorithm=md5-sess, charset=utf-8, cipher="rc4"`
	ret, err := dgst.challengeStep1([]byte(challenge))
	require.NoError(t, err)
	assert.Equal(t, []byte(`username="chris", realm="elwood.innosoft.com", nonce="OA6MG9tEQGm2hh", cnonce="OA6MHXh6VqTrRk", nc=00000001, qop=auth, digest-uri="imap/elwood.innosoft.com", response=d388dad90d4bbd760a152321f2143af7, charset=utf-8, cipher=rc4`), ret)
	assert.Equal(t, "rc4", dgst.cipher)
}

func TestMD5DigestRspAuth(t *testing.T) {
	dgst := getTestDigest()

	// setup state as it would be after the first challenge
	dgst.token = &sasl.Challenge{
		Algorithm: "md5-sess",
		Charset:   "utf-8",
		Nonce:     "OA6MG9tEQGm2hh",
		Qop:       sasl.QopAuthentication,
		Realm:     "elwood.innosoft.com",
	}
	dgst.cnonce = "OA6MHXh6VqTrRk"

	// evaluate the rspauth as per the example in RFC 2831
	err := dgst.challengeStep2([]byte("rspauth=ea40f60335c427b5527b84dbabcdfffd"))
	assert.NoError(t, err)
}

func TestDigestMD5Conn(t *testing.T) {
	// This was captured from a test connection.
	key := &hdfs.DataEncryptionKeyProto{}
	key.EncryptionKey = []byte{
		0x4d, 0xed, 0xaa, 0xd4, 0xf0, 0xf8, 0xec, 0x7d,
		0xfd, 0xf7, 0x76, 0xaf, 0xbc, 0x93, 0xba, 0x8e,
		0xd1, 0xc3, 0xb3, 0xb7}
	key.Nonce = []byte{0x79, 0x0c, 0xc3, 0xa6, 0x31, 0x7f, 0x5b, 0xd7}
	key.KeyId = proto.Uint32(388373981)
	key.BlockPoolId = proto.String("BP-529865118-10.129.176.136-1582635112897")

	empty := ""
	blockKind := "HDFS_BLOCK_TOKEN"
	token := &hadoop.TokenProto{}
	token.Kind = &blockKind
	token.Service = &empty

	origGenCnonce := genCnonce
	genCnonce = func() (string, error) {
		return "dqNZ/hGooPsuK3iWPeDFeQ==", nil
	}
	defer func() {
		genCnonce = origGenCnonce
	}()

	server, client := net.Pipe()
	serverDone := make(chan struct{})
	go func() {
		defer server.Close()
		defer close(serverDone)

		// The handshake starts by first passing 0xDEADBEEF to the server, along
		// with an empty message. We have to read as much as possible, since the
		// code writes everything in one go, and net.Pipe is unbuffered.
		b := make([]byte, 1024)
		server.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, err := server.Read(b)
		require.NoError(t, err)

		d := binary.BigEndian.Uint32(b[:4])
		assert.Equal(t, uint32(0xDEADBEEF), d)

		// In practice, this chokes since the client sends an empty message with
		// the handshake initialization.
		// server.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		// msg := &hdfs.DataTransferEncryptorMessageProto{}
		// err = readPrefixedMessage(bytes.NewReader(b[4:]), msg)
		// require.NoError(t, err)
		// assert.Equal(t, hdfs.DataTransferEncryptorMessageProto_SUCCESS.Enum(), msg.Status)

		// the server then responds with the initial challenge
		resp := &hdfs.DataTransferEncryptorMessageProto{}
		resp.Status = hdfs.DataTransferEncryptorMessageProto_SUCCESS.Enum()
		resp.Payload = []byte(`username="388373981 BP-529865118-10.129.176.136-1582635112897 eQzDpjF/W9c=", realm="0", nonce="8iQSCAmYohP0K4dBX4Z2cxYC4CFJjfVp3aATEHNN", qop="auth-conf", cipher="rc4"`)
		data, err := makePrefixedMessage(resp)
		require.NoError(t, err)

		server.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		_, err = server.Write(data)
		require.NoError(t, err)

		server.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		msg := &hdfs.DataTransferEncryptorMessageProto{}
		err = readPrefixedMessage(server, msg)
		require.NoError(t, err)

		// our client should respond appropriately with the correct challenge response
		assert.Equal(t, `username="388373981 BP-529865118-10.129.176.136-1582635112897 eQzDpjF/W9c=", realm="0", nonce="8iQSCAmYohP0K4dBX4Z2cxYC4CFJjfVp3aATEHNN", cnonce="dqNZ/hGooPsuK3iWPeDFeQ==", nc=00000001, qop=auth-conf, digest-uri="hdfs/0", response=c4669d46e21197923d3e98e53e6dd543, charset=utf-8, cipher=rc4`,
			string(msg.Payload))

		// finally the server responds with a rspauth and the cipher information
		msg.Status = hdfs.DataTransferEncryptorMessageProto_SUCCESS.Enum()
		msg.Payload = []byte("rspauth=830abc648a95a91e9ff1d594cdbca222")
		opt := &hdfs.CipherOptionProto{}
		opt.Suite = hdfs.CipherSuiteProto_AES_CTR_NOPADDING.Enum()
		// these are the encoded cipher keys, InKey and OutKey will need to be
		// decoded by the client before they can be used
		opt.InKey = []byte{
			0xbb, 0x5e, 0xcf, 0x32, 0x55, 0xe7, 0x59, 0x5b,
			0xe5, 0xf9, 0xd7, 0xd2, 0x1e, 0x29, 0xb8, 0xeb,
			0x04, 0x93, 0x8b, 0x74, 0x58, 0xbd, 0x77, 0x79,
			0x8f, 0xfd, 0xf2, 0xe3, 0xb9, 0xbd, 0x70, 0xa7,
			0x3b, 0xbc, 0xf4, 0xa2, 0xf3, 0xa1, 0x8a, 0x51,
			0x83, 0x3e, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}
		opt.InIv = []byte{
			0xc9, 0x50, 0x0c, 0xa0, 0xcc, 0x10, 0x13, 0x37,
			0x06, 0x21, 0x1e, 0x76, 0xf8, 0x64, 0xea, 0x37,
		}
		opt.OutKey = []byte{
			0x63, 0x50, 0x62, 0xfe, 0x18, 0xed, 0xb9, 0xf6,
			0x27, 0x92, 0x45, 0x6f, 0xa6, 0xdc, 0x9c, 0x6e,
			0x71, 0x5e, 0x4a, 0xcb, 0x92, 0x97, 0xa4, 0xcb,
			0xa1, 0x56, 0xe3, 0x4f, 0x25, 0x5d, 0xfb, 0xd1,
			0x65, 0x81, 0x12, 0xe5, 0xd9, 0xe0, 0x12, 0x33,
			0x53, 0xef, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
		}
		opt.OutIv = []byte{
			0xe2, 0xcb, 0xcd, 0xe2, 0x03, 0x20, 0x8e, 0x37,
			0x74, 0x02, 0x11, 0x66, 0x66, 0x9c, 0xd9, 0xa0,
		}
		msg.CipherOption = []*hdfs.CipherOptionProto{opt}

		data, _ = makePrefixedMessage(msg)
		_, err = server.Write(data)
		require.NoError(t, err)

		actualInKey := []byte{
			0xe6, 0xfb, 0x59, 0xb1, 0x7e, 0xd7, 0xdf, 0x11,
			0x3a, 0xf3, 0xac, 0x62, 0xef, 0xc0, 0x86, 0x3d,
			0x92, 0x74, 0x7d, 0xd9, 0x3f, 0xae, 0xbc, 0x62,
			0xf2, 0xb5, 0x68, 0x7b, 0x10, 0x6f, 0xa3, 0x53,
		}
		actualInIv := opt.OutIv
		actualOutKey := []byte{
			0x7b, 0x91, 0xb6, 0x66, 0x60, 0xab, 0xff, 0x8c,
			0x80, 0x48, 0xe2, 0x0c, 0xef, 0x24, 0x0c, 0xc9,
			0x0b, 0xc5, 0xd7, 0x92, 0x14, 0x9c, 0x6f, 0xea,
			0xb9, 0x12, 0x1a, 0x48, 0xc4, 0x85, 0x5f, 0x43,
		}
		actualOutIv := opt.InIv

		wrapped, _ := newAesConn(server, actualInKey, actualOutKey, actualInIv, actualOutIv)

		// Receive an encrypted value.
		wrapped.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		b = make([]byte, 4)
		_, err = wrapped.Read(b)
		require.NoError(t, err)
		assert.Equal(t, b, []byte{0xDE, 0xAD, 0xBE, 0xEF})
	}()

	wrapped, err := (&SaslDialer{Token: token, Key: key}).wrapDatanodeConn(client)
	require.NoError(t, err)
	defer wrapped.Close()

	require.NoError(t, err)

	// Send an encrypted value.
	n, err := wrapped.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	assert.NoError(t, err)
	assert.Equal(t, 4, n)

	<-serverDone
}
