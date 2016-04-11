package hdfs

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAutoClientEnvVar(t *testing.T) {
	_, err := GetNamenodeFromConfig()
	assert.Nil(t, err)
}

func TestAutoClientHadoopHome(t *testing.T) {
	pwd, _ := os.Getwd()
	os.Setenv("HADOOP_HOME", strings.Join([]string{pwd, "test"}, "/"))
	nn, _ := GetNamenodeFromConfig()
	require.Contains(t, []string{"hadoop-namenode-01:8020", "hadoop-namenode-02:8020"}, nn)
	os.Setenv("HADOOP_HOME", "")
}

func TestAutoClientHadoopConfDir(t *testing.T) {
	pwd, _ := os.Getwd()
	os.Setenv("HADOOP_CONF_DIR", strings.Join([]string{pwd, "test"}, "/"))
	nn, _ := GetNamenodeFromConfig()
	assert.EqualValues(t, "testnode:9000", nn)
	os.Setenv("HADOOP_CONF_DIR", "")
}
