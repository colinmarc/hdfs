package hdfs

import (
	"encoding/xml"
	"errors"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Property is the struct representation of hadoop configuration
// key value pair.
type Property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type propertyList struct {
	Property []Property `xml:"property"`
}

// HadoopConf represents a map of all the key value configutation
// pairs found in a user's hadoop configuration files.
type HadoopConf map[string]string

var errUnresolvedNamenode = errors.New("no namenode address in configuration")
var errInvalidHDFSFilename = errors.New("invalid HDFS Filename")

// LoadHadoopConf returns a HadoopConf object representing configuration from
// the specified path, or finds the correct path in the environment. If
// path or the env variable HADOOP_CONF_DIR is specified, it should point
// directly to the directory where the xml files are. If neither is specified,
// ${HADOOP_HOME}/conf will be used.
func LoadHadoopConf(path string) HadoopConf {

	if path == "" {
		path = os.Getenv("HADOOP_CONF_DIR")
		if path == "" {
			path = filepath.Join(os.Getenv("HADOOP_HOME"), "conf")
		}
	}

	hadoopConf := make(HadoopConf)
	for _, file := range []string{"core-site.xml", "hdfs-site.xml"} {
		pList := propertyList{}
		f, err := ioutil.ReadFile(filepath.Join(path, file))
		if err != nil {
			continue
		}

		err = xml.Unmarshal(f, &pList)
		if err != nil {
			continue
		}

		for _, prop := range pList.Property {
			hadoopConf[prop.Name] = prop.Value
		}
	}

	return hadoopConf
}

// Namenodes returns the default namenode hosts present in the configuration. The
// returned slice will be sorted and deduped.
// This function only works for old single NameService Hadoop conf
// It should be deprecated
func (conf HadoopConf) Namenodes() ([]string, error) {
	defNSID := conf.DefaultNSID()
	if defNSID != "" {
		return conf.AddressesByNameServiceID(defNSID)
	}

	// fallback to pick up all namenodex in XML

	nns := make(map[string]bool)
	for key, value := range conf {
		if strings.HasPrefix(key, "fs.default") {
			nnUrl, _ := url.Parse(value)
			nns[nnUrl.Host] = true
		} else if strings.HasPrefix(key, "dfs.namenode.rpc-address") {
			nns[value] = true
		}
	}

	if len(nns) == 0 {
		return nil, errUnresolvedNamenode
	}

	keys := make([]string, 0, len(nns))
	for k, _ := range nns {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	return keys, nil
}

// return the NameServiceID of defaultFS
func (conf HadoopConf) DefaultNSID() string {
	value := conf.DefaultFS()
	if value != "" {
		nnUrl, _ := url.Parse(value)
		return nnUrl.Host
	}
	return ""
}

// return the defaultFS
func (conf HadoopConf) DefaultFS() string {
	value, _ := conf["fs.defaultFS"]
	if value == "" { // fallback to deprecated form
		value, _ = conf["fs.default.name"]
	}
	return value
}

// return the HA Address of namenode
func (conf HadoopConf) AddressesByNameServiceID(nsid string) ([]string, error) {
	rets := make([]string, 0, 8)
	// for very simple
	if nsid == conf.DefaultNSID() {
		value := conf.DefaultFS()
		nnUrl, err := url.Parse(value)
		if err == nil {
			return []string{nnUrl.Host}, nil
		}
	}

	// for simple
	key := "dfs.nsidnode.rpc-address." + nsid
	addr, ok := conf[key]
	if ok {
		rets = append(rets, addr)
		return rets, nil
	}
	// for HA
	haListName := "dfs.ha.namenodes." + nsid
	haListStr, ok := conf[haListName]
	var haList []string
	haList = strings.Split(haListStr, ",")
	for _, haName := range haList {
		key := "dfs.namenode.rpc-address." + nsid + "." + haName
		addr, ok := conf[key]
		if ok && addr != "" {
			rets = append(rets, addr)
		}
	}
	// sort and return
	if len(rets) <= 0 {
		return nil, errUnresolvedNamenode
	} else {
		sort.Strings(rets)
		return rets, nil
	}
}

// return the actual path on the final namenode of a viewfs path
// if property
//   fs.viewfs.mounttable.nsX.link./user = hdfs://SunshineNameNode3/user2
//   defaultFS = nsX
// then
//  call ("/user/sub") returns ("SunshineNameNode3", "/user/sub", nil)
//  call ("hdfs://nsX/user/sub") returns ("SunshineNameNode3", "/user2/sub", nil)
func (conf HadoopConf) ViewfsReparseFilename(rootnsid string, filename string) (string, string, error) {
	var nsid, path string
	u, err := url.Parse(filename)
	if err != nil || (u.Scheme != "hdfs" && u.Scheme != "") {
		return "", "", errInvalidHDFSFilename
	}
	if u.Host != "" && rootnsid != "" { // host and nsid conflict
		return "", "", errInvalidHDFSFilename
	}
	nsid, path = u.Host, u.Path
	if nsid == "" {
		nsid = rootnsid
	}
	if nsid == "" {
		nsid = conf.DefaultNSID()
	}

	dirs := strings.Split(path, "/")
	if dirs[0] != "" {
		dirs = append([]string{""}, dirs...)
	}
	for i := len(dirs); i > 0; i-- {
		prefix := strings.Join(dirs[0:i], "/")
		key := "fs.viewfs.mounttable." + nsid + ".link." + prefix
		value, ok := conf[key]
		if ok {
			postfix := filepath.Join(dirs[i:]...)
			newurl := value + "/" + postfix
			u, _ = url.Parse(newurl)
			return u.Host, u.Path, nil
		}
	}
	return nsid, path, nil
}

// type of namenode address string
type TypeOfNamenodeAddressString int

const (
	_ TypeOfNamenodeAddressString = iota
	TNAS_SimpleAddress
	TNAS_SimpleNameServiceID
	TNAS_ViewfsNameServiceID
)

// returns the type of namenode/address
// 3 situation
// A. Just a simple hostname (with port)
// B. A NameServiceID without viewfs mount links
// C. A NameServiceID with viewfs mount links
func (conf HadoopConf) CheckTypeOfNameAddressString(maybe_addr string) TypeOfNamenodeAddressString {
	// if address is "Host:Port" style
	if strings.Contains(maybe_addr, ":") {
		return TNAS_SimpleAddress
	}
	// check is there any mounttable
	prefix := "fs.viewfs.mounttable." + maybe_addr + ".link."
	for key, _ := range conf {
		if strings.HasPrefix(key, prefix) {
			return TNAS_ViewfsNameServiceID
		}
	}
	// check is it a NameServiceID
	nsids_str, _ := conf["dfs.nameservices"]
	nsids := strings.Split(nsids_str, ",")
	for _, v := range nsids {
		if maybe_addr == v {
			return TNAS_SimpleNameServiceID
		}
	}
	// check is there any HA nodes
	if _, ok := conf["dfs.ha.namenodes."+maybe_addr]; ok {
		return TNAS_SimpleNameServiceID
	}
	// fallback to simple address
	return TNAS_SimpleAddress
}
