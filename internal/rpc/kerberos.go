package rpc

import (
	"errors"
	"fmt"
	"net"
	"regexp"

	hadoop "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
	"gopkg.in/jcmturner/gokrb5.v5/gssapi"
	"gopkg.in/jcmturner/gokrb5.v5/iana/keyusage"
	krbtypes "gopkg.in/jcmturner/gokrb5.v5/types"
)

const (
	// All SASL requests/responses use this sequence number.
	saslRpcCallId = -33
)

var (
	errKerberosNotSupported = errors.New("kerberos authentication not supported by namenode")
	krbSPNHost              = regexp.MustCompile(`\A[^/]+/(_HOST)([@/]|\z)`)
)

func (c *NamenodeConnection) doKerberosHandshake() error {
	// Start negotiation, and get the list of supported mechanisms in reply.
	err := c.writeSaslRequest(&hadoop.RpcSaslProto{
		State: hadoop.RpcSaslProto_NEGOTIATE.Enum(),
	})
	if err != nil {
		return err
	}

	resp, err := c.readSaslResponse(hadoop.RpcSaslProto_NEGOTIATE)
	if err != nil {
		return err
	}

	var krbAuth *hadoop.RpcSaslProto_SaslAuth
	var tokenAuth *hadoop.RpcSaslProto_SaslAuth
	for _, m := range resp.GetAuths() {
		if *m.Method == "KERBEROS" {
			krbAuth = m
		}
		if *m.Method == "TOKEN" {
			tokenAuth = m
		}
	}

	if krbAuth == nil {
		return errKerberosNotSupported
	}

	// Get a ticket from Kerberos, and send the initial token to the namenode.
	token, sessionKey, err := c.getKerberosTicket()
	if err != nil {
		return err
	}
	c.currentSessionKey = sessionKey

	if tokenAuth != nil {
		challenge, err := ParseChallenge(tokenAuth)
		if err != nil {
			return err
		}
		switch challenge.QOP {
		case Privacy, Integrity:
			// Switch to SASL RPC handler
			c.rpcReader = &SaslRpcReader{
				SessionKey:      sessionKey,
				Confidentiality: challenge.QOP == Privacy,
			}
		case Authentication:
			// just use default RPC handler
		default:
			return errors.New("unexpected QOP")
		}
	}

	err = c.writeSaslRequest(&hadoop.RpcSaslProto{
		State: hadoop.RpcSaslProto_INITIATE.Enum(),
		Token: token.MechToken,
		Auths: []*hadoop.RpcSaslProto_SaslAuth{krbAuth},
	})
	if err != nil {
		return err
	}

	// In response, we get a server token to verify.
	resp, err = c.readSaslResponse(hadoop.RpcSaslProto_CHALLENGE)
	if err != nil {
		return err
	}

	var nnToken gssapi.WrapToken
	err = nnToken.Unmarshal(resp.GetToken(), true)
	if err != nil {
		return err
	}

	_, err = nnToken.VerifyCheckSum(sessionKey, keyusage.GSSAPI_ACCEPTOR_SEAL)
	if err != nil {
		return fmt.Errorf("invalid server token: %s", err)
	}

	// Sign the payload and send it back to the namenode.
	// TODO: Make sure we can support what is required based on what's in the
	// payload.
	signed, err := gssapi.NewInitiatorToken(nnToken.Payload, sessionKey)
	if err != nil {
		return err
	}

	signedBytes, err := signed.Marshal()
	if err != nil {
		return err
	}

	err = c.writeSaslRequest(&hadoop.RpcSaslProto{
		State: hadoop.RpcSaslProto_RESPONSE.Enum(),
		Token: signedBytes,
	})
	if err != nil {
		return err
	}

	// Read the final response. If it's a SUCCESS, then we're done here.
	_, err = c.readSaslResponse(hadoop.RpcSaslProto_SUCCESS)
	return err
}

func (c *NamenodeConnection) writeSaslRequest(req *hadoop.RpcSaslProto) error {
	rrh := newRPCRequestHeader(saslRpcCallId, c.ClientID)
	packet, err := makeRPCPacket(rrh, req)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(packet)
	return err
}

func (c *NamenodeConnection) readSaslResponse(expectedState hadoop.RpcSaslProto_SaslState) (*hadoop.RpcSaslProto, error) {
	rrh := &hadoop.RpcResponseHeaderProto{}
	resp := &hadoop.RpcSaslProto{}
	err := readRPCPacket(c.conn, rrh, resp)
	if err != nil {
		return nil, err
	} else if int32(rrh.GetCallId()) != saslRpcCallId {
		return nil, errors.New("unexpected sequence number")
	} else if rrh.GetStatus() != hadoop.RpcResponseHeaderProto_SUCCESS {
		return nil, &NamenodeError{
			method:    "sasl",
			message:   rrh.GetErrorMsg(),
			code:      int(rrh.GetErrorDetail()),
			exception: rrh.GetExceptionClassName(),
		}
	} else if resp.GetState() != expectedState {
		return nil, fmt.Errorf("unexpected SASL state: %s", resp.GetState().String())
	}

	return resp, nil
}

// getKerberosTicket returns an initial kerberos negotiation token and the
// paired session key, along with an error if any occured.
func (c *NamenodeConnection) getKerberosTicket() (gssapi.NegTokenInit, krbtypes.EncryptionKey, error) {
	host, _, _ := net.SplitHostPort(c.host.address)
	spn := replaceSPNHostWildcard(c.kerberosServicePrincipleName, host)

	ticket, key, err := c.kerberosClient.GetServiceTicket(spn)
	if err != nil {
		return gssapi.NegTokenInit{}, key, err
	}

	token, err := gssapi.NewNegTokenInitKrb5(*c.kerberosClient.Credentials, ticket, key)
	return token, key, err
}

// replaceSPNHostWildcard substitutes the special string '_HOST' in the given
// SPN for the given (current) host.
func replaceSPNHostWildcard(spn, host string) string {
	res := krbSPNHost.FindStringSubmatchIndex(spn)
	if res == nil || res[2] == -1 {
		return spn
	}

	return spn[:res[2]] + host + spn[res[3]:]
}
