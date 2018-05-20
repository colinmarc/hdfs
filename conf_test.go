package hdfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfFallback(t *testing.T) {
	os.Setenv("HADOOP_HOME", "test") // This will resolve to test/conf.
	os.Setenv("HADOOP_CONF_DIR", "test/conf2")

	confNamenodes := []string{"namenode1:8020", "namenode2:8020"}
	conf2Namenodes := []string{"namenode3:8020"}
	conf3Namenodes := []string{"namenode4:8020"}

	conf := LoadHadoopConf("test/conf3")
	nns, err := conf.Namenodes()
	assert.Nil(t, err)
	assert.EqualValues(t, conf3Namenodes, nns, "loading via specified path (test/conf3)")

	conf = LoadHadoopConf("")
	nns, err = conf.Namenodes()
	assert.Nil(t, err)
	assert.EqualValues(t, conf2Namenodes, nns, "loading via HADOOP_CONF_DIR (test/conf2)")

	os.Unsetenv("HADOOP_CONF_DIR")

	conf = LoadHadoopConf("")
	nns, err = conf.Namenodes()
	assert.Nil(t, err)
	assert.EqualValues(t, confNamenodes, nns, "loading via HADOOP_HOME (test/conf)")

	os.Unsetenv("HADOOP_HOME")
}

func TestConfCheckTypeOfNameAddressString(t *testing.T) {
	var ty TypeOfNamenodeAddressString

	os.Setenv("HADOOP_HOME", "test") // This will resolve to test/conf.
	os.Setenv("HADOOP_CONF_DIR", "test/conf-viewfs")

	conf := LoadHadoopConf("")

	ty = conf.CheckTypeOfNameAddressString("nsX")
	assert.EqualValues(t, TNAS_ViewfsNameServiceID, ty, "check addr type in (test/conf-viewfs)")

	ty = conf.CheckTypeOfNameAddressString("nsY")
	assert.EqualValues(t, TNAS_ViewfsNameServiceID, ty, "check addr type in (test/conf-viewfs)")

	ty = conf.CheckTypeOfNameAddressString("localhost:8080")
	assert.EqualValues(t, TNAS_SimpleAddress, ty, "check addr type in (test/conf-viewfs)")

	ty = conf.CheckTypeOfNameAddressString("SunshineNameNode1")
	assert.EqualValues(t, TNAS_SimpleNameServiceID, ty, "check addr type in (test/conf-viewfs)")

	os.Unsetenv("HADOOP_CONF_DIR")
	os.Unsetenv("HADOOP_HOME")
}

func TestConfWithViewfs(t *testing.T) {
	var nns []string
	var err error
	var newnsid, newpath string

	os.Setenv("HADOOP_HOME", "test") // This will resolve to test/conf.
	os.Setenv("HADOOP_CONF_DIR", "test/conf-viewfs")

	snn2Addrs := []string{"localhost:9000", "localhost:9001"}

	conf := LoadHadoopConf("")

	defNsid := conf.DefaultNSID()
	assert.EqualValues(t, "nsX", defNsid, "check defaultNSID in (test/conf-viewfs)")

	nns, err = conf.Namenodes()
	assert.Nil(t, err)
	assert.EqualValues(t, []string{"nsX"}, nns, "loading via specified path (test/conf-viewfs)")

	nns, err = conf.AddressesByNameServiceID("SunshineNameNode2")
	assert.Nil(t, err)
	assert.EqualValues(t, snn2Addrs, nns, "loading via specified path (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("nsX", "/norm")
	assert.Nil(t, err)
	assert.EqualValues(t, "nsX", newnsid, "loading via specified path (test/conf-viewfs)")
	assert.EqualValues(t, "/norm", newpath, "loading via specified path (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("", "hdfs://nsX/cloud/sub")
	assert.Nil(t, err)
	assert.EqualValues(t, "SunshineNameNode1", newnsid, "loading via specified path (test/conf-viewfs)")
	assert.EqualValues(t, "/_cloud/sub", newpath, "loading via specified path (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("nsX", "/user/sub")
	assert.Nil(t, err)
	assert.EqualValues(t, "SunshineNameNode2", newnsid, "loading via specified path (test/conf-viewfs)")
	assert.EqualValues(t, "/_user/sub", newpath, "loading via specified path (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("", "/user/sub")
	assert.Nil(t, err)
	assert.EqualValues(t, "SunshineNameNode2", newnsid, "loading via specified path (test/conf-viewfs)")
	assert.EqualValues(t, "/_user/sub", newpath, "loading via specified path (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("nsY", "/app/sub")
	assert.Nil(t, err)
	assert.EqualValues(t, "SunshineNameNode3", newnsid, "loading via specified path (test/conf-viewfs)")
	assert.EqualValues(t, "/_app/sub", newpath, "loading via specified path (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("", "hdfs://nsZ/app/sub")
	assert.Nil(t, err)
	assert.EqualValues(t, "nsZ", newnsid, "loading via specified path (test/conf-viewfs)")
	assert.EqualValues(t, "/app/sub", newpath, "loading via specified path (test/conf-viewfs)")

	os.Unsetenv("HADOOP_CONF_DIR")
	os.Unsetenv("HADOOP_HOME")
}
