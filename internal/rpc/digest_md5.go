package rpc

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math"
	"math/rand"
	"net"
	"strings"

	"github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
)

// EncryptionKey contains the information from the Namenode EncryptionKeys
// which will be used to establish sasl handshakes
type EncryptionKey struct {
	KeyID         int32
	ExpiryDate    uint64
	BlockPoolID   string
	Nonce         []byte
	EncryptionKey []byte
	EncryptionAlg string
}

// our default auth configuration
var (
	authMethod    = "TOKEN"
	authMechanism = "DIGEST-MD5"
	authServer    = "0"
	authProtocol  = "hdfs"
)

// some constants for handling the encoding and decoding of
// data for sasl communication
const (
	saslIntegrityPrefixLength = 4
	macDataLen                = 4
	macHMACLen                = 10
	macMsgTypeLen             = 2
	macSeqNumLen              = 4
)

var macMsgType = [2]byte{0x00, 0x01}

// lenEncodeBytes writes the input integer encoded as a 4 byte slice.
func lenEncodeBytes(seqnum int) (out [4]byte) {
	out[0] = byte((seqnum >> 24) & 0xFF)
	out[1] = byte((seqnum >> 16) & 0xFF)
	out[2] = byte((seqnum >> 8) & 0xFF)
	out[3] = byte(seqnum & 0xFF)
	return
}

type dataTransferStatus *hdfs.DataTransferEncryptorMessageProto_DataTransferEncryptorStatus

// sendSaslMsg is a helper function to encode and write a message for our sasl communication
func sendSaslMsg(w io.Writer, status dataTransferStatus, payload []byte, message string, secure bool) error {
	msg := &hdfs.DataTransferEncryptorMessageProto{}
	msg.Status = status
	msg.Payload = payload
	msg.Message = &message

	if secure {
		// if we want a secure connection, tell the server that we want AES
		opt := &hdfs.CipherOptionProto{}
		opt.Suite = hdfs.CipherSuiteProto_AES_CTR_NOPADDING.Enum()
		msg.CipherOption = append(msg.CipherOption, opt)
	}

	data, err := makePrefixedMessage(msg)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	return err
}

// digestMD5 holds the information for the token-digestmd5 authentication
// flow for us to handle the challenges and get the integrity and privacy
// key pairs for encoding and decoding messages for the server
type digestMD5 struct {
	authID   []byte
	passwd   string
	hostname string
	service  string

	token *tokenChallenge

	cnonce string
	cipher cipherType
}

// convenience function to calling the a1 function for evaluating challenges
func (d *digestMD5) a1() string {
	return a1(string(d.authID), d.token.realm, d.passwd, d.token.nonce, d.cnonce)
}

// convenience function for calling the a2 function for evaluating challenges
func (d *digestMD5) a2(first bool) string {
	return a2(d.service+"/"+d.hostname, d.token.qop, first)
}

// challengeStep1 implements Using Digest Authentication as a SASAL Mechanism
// as per RFC2831.
func (d *digestMD5) challengeStep1(challenge []byte) (string, error) {
	ncval := fmt.Sprintf("%08x", 1)
	var err error
	d.token, err = parseChallenge(challenge)
	if err != nil {
		return "", err
	}

	d.cnonce, err = genCnonce()
	if err != nil {
		return "", err
	}

	d.cipher = chooseCipher(d.token.cipher)
	rspdigest := compute(d.a1(), d.a2(true), d.token.nonce, d.cnonce, ncval, d.token.qop)

	ret := fmt.Sprintf(`username="%s", realm="%s", nonce="%s", cnonce="%s", nc=%08x, qop=%s, digest-uri="%s/%s", response=%s, charset=utf-8`,
		d.authID, d.token.realm, d.token.nonce, d.cnonce, 1, d.token.qop, d.service, d.hostname, rspdigest)

	if d.cipher != cipherUnknown {
		ret += ", cipher=" + d.cipher.String()
	}
	return ret, nil
}

// challengeStep2 implements Using Digest Authentication as a SASAL Mechanism
// as per RFC2831.
func (d *digestMD5) challengeStep2(challenge []byte) (string, error) {
	ncval := fmt.Sprintf("%08x", 1)
	rspdigest := compute(d.a1(), d.a2(false), d.token.nonce, d.cnonce, ncval, d.token.qop)

	rspauth := strings.Split(string(challenge), "=")
	if rspauth[0] != "rspauth" {
		return "", fmt.Errorf("Could not find rspauth in '%s'", string(challenge))
	}
	if rspauth[1] != rspdigest {
		return "", fmt.Errorf("rspauth did not match digest")
	}
	return "", nil
}

// make this easy to mock out for testing by having it defined here where
// unit tests can override it
var genCnonce = func() (string, error) {
	ret := make([]byte, 12)
	if _, err := rand.Read(ret); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ret), nil
}

// newDigestMD5Conn performs the necessary handshake based on the configuration and then returns
// a wrapped connection or error, in the case of our auth set to only authentication, then it just
// returns the connection unmodified after authenticating
func newDigestMD5Conn(conn net.Conn, token *hadoop_common.TokenProto, key *EncryptionKey, useSecure bool) (net.Conn, error) {
	auth := &hadoop_common.RpcSaslProto_SaslAuth{}
	auth.Method = &authMethod
	auth.Mechanism = &authMechanism
	auth.ServerId = &authServer
	auth.Protocol = &authProtocol

	ourToken := &hadoop_common.TokenProto{}
	ourToken.Kind = token.Kind
	ourToken.Password = token.Password[:]
	ourToken.Service = token.Service
	ourToken.Identifier = token.GetIdentifier()

	// if the server didn't send us a Nonce, then the data isn't encrypted
	// but we will still attempt to authenticate
	if useSecure && len(key.Nonce) == 0 {
		useSecure = false
	}

	if useSecure {
		nonceBase64Len := int(math.Ceil(4 * (float64(len(key.Nonce)) / 3)))
		buf := new(bytes.Buffer)
		buf.Grow(6 + nonceBase64Len + len(key.BlockPoolID))
		buf.WriteString(fmt.Sprintf("%d", key.KeyID))
		buf.WriteString(" ")
		buf.WriteString(key.BlockPoolID)
		buf.WriteString(" ")
		buf.WriteString(base64.StdEncoding.EncodeToString(key.Nonce))
		ourToken.Identifier = buf.Bytes()
		ourToken.Password = key.EncryptionKey
	} else {
		ourToken.Identifier = make([]byte, base64.StdEncoding.EncodedLen(len(token.GetIdentifier())))
		base64.StdEncoding.Encode(ourToken.Identifier, token.GetIdentifier())
	}

	dgst := digestMD5{
		authID:   ourToken.Identifier,
		passwd:   base64.StdEncoding.EncodeToString(ourToken.Password),
		hostname: auth.GetServerId(),
		service:  auth.GetProtocol(),
	}

	// start handshake
	var err error
	if err = binary.Write(conn, binary.BigEndian, uint32(0xDEADBEEF)); err != nil {
		return nil, err
	}

	if err = sendSaslMsg(conn, hdfs.DataTransferEncryptorMessageProto_SUCCESS.Enum(), []byte{}, "", false); err != nil {
		return nil, err
	}

	msg := &hdfs.DataTransferEncryptorMessageProto{}
	if err = readPrefixedMessage(conn, msg); err != nil {
		return nil, err
	}

	challengeResponse, err := dgst.challengeStep1(msg.Payload)
	if err != nil {
		return nil, err
	}

	if err = sendSaslMsg(conn, hdfs.DataTransferEncryptorMessageProto_SUCCESS.Enum(), []byte(challengeResponse), "", useSecure); err != nil {
		return nil, err
	}

	if err = readPrefixedMessage(conn, msg); err != nil {
		return nil, err
	}

	// the result of this challenge evaluation should be an empty string
	if challengeResponse, err = dgst.challengeStep2(msg.Payload); err != nil || len(challengeResponse) != 0 {
		return nil, err
	}

	// we're only authenticating, no RPC protection involved, so no wrapping the connection
	// just return the connection as is, we're done here!
	if dgst.token.qop == qopAuthentication {
		return conn, nil
	}

	kic, kis := generateIntegrityKeyPair(dgst.a1())

	var d digestMD5Handler
	if dgst.token.qop == qopPrivacy {
		if dgst.cipher == cipherUnknown {
			return nil, fmt.Errorf("Could not find implemented cipher among choices: %v", dgst.token.cipher)
		}
		kcc, kcs := generatePrivacyKeyPair(dgst.a1(), dgst.cipher)
		d, err = newDigestMD5PrivacyConn(conn, kic[:], kis[:], kcc[:], kcs[:])
		if err != nil {
			return nil, err
		}
	} else if dgst.token.qop == qopIntegrity {
		d = newDigestMD5IntegrityConn(conn, kic[:], kis[:])
	}

	if len(msg.GetCipherOption()) > 0 {
		cipher := msg.GetCipherOption()[0]
		var outKey []byte

		decoded, err := d.decode(cipher.InKey)
		if err != nil {
			return nil, err
		}
		// we reuse the buffer that is returned from decoding, so we need to copy
		// the decoded output so the next call to decode doesn't blow it away
		inKey := make([]byte, len(decoded))
		copy(inKey, decoded)

		if outKey, err = d.decode(cipher.OutKey); err != nil {
			return nil, err
		}

		return newAesConn(conn, inKey, outKey, cipher.InIv, cipher.OutIv)
	}
	return d, nil
}

// helper interface for decoding the cipher keys if we need to construct
// an aes connection
type digestMD5Handler interface {
	net.Conn
	decode(input []byte) ([]byte, error)
}

// compute implements the computation of md5 digest authentication as per RFC2831
// using the same terms as described in the RFC in order to make it easier to
// understand and ensure it maintains proper functionality. The response value
// computation is defined as:
//
//     HEX( KD ( HEX(H(A1)), { nonce-value, ":", nc-value, ":", cnonce-value, ":", qop-value, ":", HEX(H(A2)) }))
//
//     A1 = { H( { username-value, ":", realm-value, ":", passwd }), ":", nonce-value, ":", cnonce-value }
//
//   If "qop" is "auth":
//
//		 A2 = { "AUTHENTICATE:", digest-uri-value }
//
//   If "qop" is "auth-int" or "auth-conf":
//
//     A2 = { "AUTHENTICATE:", digest-uri-value, ":00000000000000000000000000000000" }
//
//   Where:
//
//     { a, b, ... } is the concatenation of the octet strings a, b, ...
//
//     H(s) is the 16 octet MD5 Hash [RFC1321] of the octet string s
//
//     KD(k, s) is H({k, ":", s}), i.e., the 16 octet hash of the string k, a colon, and the string s
//
//     HEX(n) is the representation of the 16 octet MD5 hash n as a string of 32 hex digits (with alphabetic characters
//     in lower case)
func compute(a1, a2, nonce, cnonce, ncvalue, qop string) string {
	x := hexfn(h(a1))
	y := strings.Join([]string{nonce, ncvalue, cnonce, qop, hexfn(h(a2))}, ":")
	return hexfn(kd(x, y))
}

func h(s string) [md5.Size]byte {
	return md5.Sum([]byte(s))
}

func kd(k, s string) [md5.Size]byte {
	return h(strings.Join([]string{k, s}, ":"))
}

func hexfn(data [md5.Size]byte) string {
	return hex.EncodeToString(data[:])
}

func a1(username, realm, password, nonce, cnonce string) string {
	x := h(strings.Join([]string{username, realm, password}, ":"))
	return strings.Join([]string{string(x[:]), nonce, cnonce}, ":")
}

func a2(digestURI, qop string, initial bool) string {
	var a2 string
	// when computing the initial response digest, we use the "AUTHENTICATE" piece
	// but when validating the server response-auth, the "AUTHENTICATE:" string is
	// left out in taht calculation to confirm the digest-response
	if initial {
		a2 = strings.Join([]string{"AUTHENTICATE", digestURI}, ":")
	} else {
		a2 = ":" + digestURI
	}
	if qop == qopPrivacy || qop == qopIntegrity {
		a2 = a2 + ":00000000000000000000000000000000"
	}
	return a2
}

// If the server offered qop=auth-int and we replied with qop=auth-int then subsequent
// messages need to be integrity protected. The base session key is H(A1) as defined above
// this function will generate the pair of integrity keys for both client to server (kic)
// and server to client (kis) as defined in the RFC with the specified magic constants
func generateIntegrityKeyPair(a1 string) ([md5.Size]byte, [md5.Size]byte) {
	clientIntMagicStr := []byte("Digest session key to client-to-server signing key magic constant")
	serverIntMagicStr := []byte("Digest session key to server-to-client signing key magic constant")

	sum := h(a1)
	kic := md5.Sum(append(sum[:], clientIntMagicStr...))
	kis := md5.Sum(append(sum[:], serverIntMagicStr...))

	return kic, kis
}

// If message integrity is negotiated, there will be a MAC block for each message appended
// to the message. The MAC block is 16 bytes, the first 10 bytes of the HMAC-MD5 of the message
// plus a 2 byte message type number and a 4 byte sequence number. This function provides the
// HMAC as defined in the RFC as HMAC(ki, {seqnum, msg})[0..9]
func getHMAC(mac hash.Hash, seq, msg []byte) []byte {
	data := append(seq, msg...)

	mac.Reset()
	mac.Write(data)

	hash := mac.Sum(nil)
	return hash[0:10]
}

// If the server sent a cipher-opts directive and we respond with a cipher directive, then
// subsequent messages between the client and server must be confidentiality protected via
// privacy keys. Again we use the base session key of H(A1) as defined above, and then we
// calculate the privacy keys:
//
//   For the Client-to-Server:
//
//      kcc = MD5({H(A1)[0..n], "Digest H(A1) to client-to-server sealing key magic constant"})
//
//   For the Server-to-Client:
//
//      kcs = MD5({H(A1)[0..n], "Digest H(A1) to server-to-client sealing key magic constant"})
//
//   Where n is based on the cipher we choose:
//     rc4-40: n == 5
//     rc4-56: n == 7
//     for all others n == 16
//
//   For now, I've only implemented handling for rc4, not des or 3des
func generatePrivacyKeyPair(a1 string, useCipher cipherType) ([md5.Size]byte, [md5.Size]byte) {
	clientConfMagicStr := []byte("Digest H(A1) to client-to-server sealing key magic constant")
	serverConfMagicStr := []byte("Digest H(A1) to server-to-client sealing key magic constant")

	sum := h(a1)
	var n int
	switch useCipher {
	case cipherRC440:
		n = 5
	case cipherRC456:
		n = 7
	default:
		n = md5.Size
	}

	kcc := md5.Sum(append(sum[:n], clientConfMagicStr...))
	kcs := md5.Sum(append(sum[:n], serverConfMagicStr...))

	return kcc, kcs
}
