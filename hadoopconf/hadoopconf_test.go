package hadoopconf

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfFallback(t *testing.T) {
	oldHome := os.Getenv("HADOOP_HOME")
	oldConfDir := os.Getenv("HADOOP_CONF_DIR")
	os.Setenv("HADOOP_HOME", "testdata") // This will resolve to testdata/conf.
	os.Setenv("HADOOP_CONF_DIR", "testdata/conf2")

	confNamenodes := []string{"namenode1:8020", "namenode2:8020"}
	conf2Namenodes := []string{"namenode3:8020"}

	conf, err := LoadFromEnvironment()
	assert.NoError(t, err)

	nns := conf.DefaultNamenodes()
	assert.NoError(t, err)
	assert.EqualValues(t, conf2Namenodes, nns, "loading via HADOOP_CONF_DIR (testdata/conf2)")

	os.Unsetenv("HADOOP_CONF_DIR")

	conf, err = LoadFromEnvironment()
	assert.NoError(t, err)

	nns = conf.DefaultNamenodes()
	assert.NoError(t, err)
	assert.EqualValues(t, confNamenodes, nns, "loading via HADOOP_HOME (testdata/conf)")

	os.Setenv("HADOOP_HOME", oldHome)
	os.Setenv("HADOOP_CONF_DIR", oldConfDir)
}
