package sasl

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	// QopAuthenication is how the namenode refers to authentication mode, which
	// only establishes mutual authentication without encryption (the default).
	QopAuthentication = "auth"
	// QopIntegrity is how the namenode refers to integrity mode, which, in
	// in addition to authentication, verifies the signature of RPC messages.
	QopIntegrity = "auth-int"
	// QopPrivacy is how the namenode refers to privacy mode, which, in addition
	// to authentication and integrity, provides full end-to-end encryption for
	// RPC messages.
	QopPrivacy = "auth-conf"
)

var challengeRegexp = regexp.MustCompile(",?([a-zA-Z0-9]+)=(\"([^\"]+)\"|([^,]+)),?")

type Challenge struct {
	Realm     string
	Nonce     string
	Qop       []string
	Charset   string
	Cipher    []string
	Algorithm string
}

func ParseChallenge(challenge []byte) (*Challenge, error) {
	ch := Challenge{}

	matched := challengeRegexp.FindAllSubmatch(challenge, -1)
	if matched == nil {
		return nil, fmt.Errorf("invalid token challenge: %s", challenge)
	}

	for _, m := range matched {
		key := string(m[1])
		val := string(m[3])
		switch key {
		case "realm":
			ch.Realm = val
		case "nonce":
			ch.Nonce = val
		case "qop":
			ch.Qop = strings.Split(val, ",")
		case "charset":
			ch.Charset = val
		case "cipher":
			ch.Cipher = strings.Split(val, ",")
		case "algorithm":
			ch.Algorithm = val
		default:
		}
	}

	if len(ch.Qop) == 0 {
		return nil, errors.New("invalid token challenge: no selected QOP")
	}

	return &ch, nil
}
