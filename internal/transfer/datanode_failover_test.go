package transfer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPicksFirstDatanode(t *testing.T) {
	df := newDatanodeFailover([]string{"foo:6000", "bar:6000"})
	assert.EqualValues(t, df.next(), "foo:6000")
}

func TestPicksDatanodesWithoutFailures(t *testing.T) {
	df := newDatanodeFailover([]string{"foo:6000", "foo:7000", "bar:6000"})
	datanodeFailures["foo:6000"] = time.Now()

	assert.EqualValues(t, df.next(), "foo:7000")
}

func TestPicksDatanodesWithOldestFailures(t *testing.T) {
	df := newDatanodeFailover([]string{"foo:6000", "bar:6000"})
	datanodeFailures["foo:6000"] = time.Now().Add(-10 * time.Minute)
	datanodeFailures["bar:6000"] = time.Now()

	assert.EqualValues(t, df.next(), "foo:6000")
}
