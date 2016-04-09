package hdfs

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAutoClientEnvVar(t *testing.T) {
	_, err := AutoConfigClient()
	assert.Nil(t, err)
}

func TestAutoClientHadoopHome(t *testing.T) {
	pwd, _ := os.Getwd()
	os.Setenv("HADOOP_HOME", strings.Join([]string{pwd, "test"}, "/"))
	_, err := AutoConfigClient()
	assert.NotNil(t, err)
	assert.EqualValues(t, "dial tcp: lookup hadoop-namenode-01: no such host", fmt.Sprintf("%s", err))
	os.Setenv("HADOOP_HOME", "")
}

func TestAutoClientHadoopConfDir(t *testing.T) {
	pwd, _ := os.Getwd()
	os.Setenv("HADOOP_CONF_DIR", strings.Join([]string{pwd, "test"}, "/"))
	_, err := AutoConfigClient()
	assert.NotNil(t, err)
	assert.EqualValues(t, "dial tcp: lookup testnode: no such host", fmt.Sprintf("%s", err))
	os.Setenv("HADOOP_CONF_DIR", "")
}
