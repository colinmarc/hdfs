package rpc

import (
	"errors"
	"regexp"
	"strings"

	hadoop "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
)

// QualityOfProtection is the level of security that is used when sending and receiving messages
type QualityOfProtection string

const (
	// Authentication - Establishes mutual authentication between the client and the server
	Authentication = QualityOfProtection("auth")
	// Integrity - In addition to authentication, it guarantees that a man-in-the-middle cannot tamper with messages exchanged between the client and the server
	Integrity = QualityOfProtection("auth-int")
	// Privacy - In addition to the features offered by authentication and integrity, it also fully encrypts the messages exchanged between the client and the server
	Privacy = QualityOfProtection("auth-conf")
)

var challengeRegexp = regexp.MustCompile(",?([a-zA-Z0-9]+)=(\"([^\"]+)\"|([^,]+)),?")

// TokenChallenge is a struct which holds a challenge of TOKEN auth
type TokenChallenge struct {
	Realm     string
	Nonce     string
	QOP       QualityOfProtection
	Charset   string
	Cipher    []string
	Algorithm string
}

// ParseChallenge returns a TokenChallenge parsed from a given SaslAuth
func ParseChallenge(auth *hadoop.RpcSaslProto_SaslAuth) (*TokenChallenge, error) {
	tokenChallenge := TokenChallenge{}
	matched := challengeRegexp.FindAllSubmatch(auth.Challenge, -1)
	if matched == nil {
		return nil, errors.New("aaaa")
	}
	for _, m := range matched {
		key := string(m[1])
		val := string(m[3])
		switch key {
		case "realm":
			tokenChallenge.Realm = val
		case "nonce":
			tokenChallenge.Nonce = val
		case "qop":
			tokenChallenge.QOP = QualityOfProtection(val)
		case "charset":
			tokenChallenge.Charset = val
		case "cipher":
			tokenChallenge.Cipher = strings.Split(val, ",")
		case "algorithm":
			tokenChallenge.Algorithm = val
		default:
			// skip
		}
	}
	return &tokenChallenge, nil
}
