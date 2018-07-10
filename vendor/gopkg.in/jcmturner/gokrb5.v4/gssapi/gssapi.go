// Package gssapi implements Generic Security Services Application Program Interface required for SPNEGO kerberos authentication.
package gssapi

import (
	"errors"
	"fmt"
	"github.com/jcmturner/gofork/encoding/asn1"
	"gopkg.in/jcmturner/gokrb5.v4/asn1tools"
	"gopkg.in/jcmturner/gokrb5.v4/credentials"
	"gopkg.in/jcmturner/gokrb5.v4/messages"
	"gopkg.in/jcmturner/gokrb5.v4/types"
)

// SPNEGO_OID is the OID for SPNEGO header type.
var SPNEGO_OID = asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 2}

// SPNEGO header struct
type SPNEGO struct {
	Init         bool
	Resp         bool
	NegTokenInit NegTokenInit
	NegTokenResp NegTokenResp
}

// Unmarshal SPNEGO negotiation token
func (s *SPNEGO) Unmarshal(b []byte) error {
	var r []byte
	var err error
	if b[0] != byte(161) {
		// Not a NegTokenResp/Targ could be a NegTokenInit
		var oid asn1.ObjectIdentifier
		r, err = asn1.UnmarshalWithParams(b, &oid, fmt.Sprintf("application,explicit,tag:%v", 0))
		if err != nil {
			return fmt.Errorf("Not a valid SPNEGO token: %v", err)
		}
		// Check the OID is the SPNEGO OID value
		if !oid.Equal(asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 2}) {
			return errors.New("OID does not match SPNEGO OID 1.3.6.1.5.5.2")
		}
	} else {
		// Could be a NegTokenResp/Targ
		r = b
	}

	var a asn1.RawValue
	_, err = asn1.Unmarshal(r, &a)
	if err != nil {
		return fmt.Errorf("Error unmarshalling SPNEGO: %v", err)
	}
	switch a.Tag {
	case 0:
		_, err = asn1.Unmarshal(a.Bytes, &s.NegTokenInit)
		if err != nil {
			return fmt.Errorf("Error unmarshalling NegotiationToken type %d (Init): %v", a.Tag, err)
		}
		s.Init = true
	case 1:
		_, err = asn1.Unmarshal(a.Bytes, &s.NegTokenResp)
		if err != nil {
			return fmt.Errorf("Error unmarshalling NegotiationToken type %d (Resp/Targ): %v", a.Tag, err)
		}
		s.Resp = true
	default:
		return errors.New("Unknown choice type for NegotiationToken")
	}
	return nil
}

// Marshal SPNEGO negotiation token
func (s *SPNEGO) Marshal() ([]byte, error) {
	var b []byte
	if s.Init {
		hb, _ := asn1.Marshal(SPNEGO_OID)
		tb, err := s.NegTokenInit.Marshal()
		if err != nil {
			return b, fmt.Errorf("Could not marshal NegTokenInit: %v", err)
		}
		b = append(hb, tb...)
		return asn1tools.AddASNAppTag(b, 0), nil
	}
	if s.Resp {
		b, err := s.NegTokenResp.Marshal()
		if err != nil {
			return b, fmt.Errorf("Could not marshal NegTokenResp: %v", err)
		}
		return b, nil
	}
	return b, errors.New("SPNEGO cannot be marshalled. It contains neither a NegTokenInit or NegTokenResp")
}

// GetSPNEGOKrbNegTokenInit returns an SPNEGO struct containing a NegTokenInit.
func GetSPNEGOKrbNegTokenInit(creds credentials.Credentials, tkt messages.Ticket, sessionKey types.EncryptionKey) (SPNEGO, error) {
	negTokenInit, err := NewNegTokenInitKrb5(creds, tkt, sessionKey)
	if err != nil {
		return SPNEGO{}, fmt.Errorf("Could not create NegTokenInit: %v", err)
	}
	return SPNEGO{
		Init:         true,
		NegTokenInit: negTokenInit,
	}, nil
}
