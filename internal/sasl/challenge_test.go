package sasl

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortQops(t *testing.T) {
	allQops := Qops{QopAuthentication, QopIntegrity, QopPrivacy}
	sort.Sort(allQops)
	assert.Equal(t, Qops{QopPrivacy, QopIntegrity, QopAuthentication}, allQops)

	includeInvalidQops := Qops{QopAuthentication, QopPrivacy, "invalid"}
	sort.Sort(includeInvalidQops)
	assert.Equal(t, Qops{QopPrivacy, QopAuthentication, "invalid"}, includeInvalidQops)
}
