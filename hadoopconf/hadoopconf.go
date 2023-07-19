// Package hadoopconf provides utilities for reading and parsing Hadoop's xml
// configuration files.
package hadoopconf

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type propertyList struct {
	Property []property `xml:"property"`
}

var confFiles = []string{"core-site.xml", "hdfs-site.xml", "mapred-site.xml"}

// HadoopConf represents a map of all the key value configutation
// pairs found in a user's hadoop configuration files.
type HadoopConf map[string]string

// LoadFromEnvironment tries to locate the Hadoop configuration files based on
// the environment, and returns a HadoopConf object representing the parsed
// configuration. If the HADOOP_CONF_DIR environment variable is specified, it
// uses that, or if HADOOP_HOME is specified, it uses $HADOOP_HOME/conf.
//
// If no configuration can be found, it returns a nil map. If the configuration
// files exist but there was an error opening or parsing them, that is returned
// as well.
func LoadFromEnvironment() (HadoopConf, error) {
	hadoopConfDir := os.Getenv("HADOOP_CONF_DIR")
	if hadoopConfDir != "" {
		if conf, err := Load(hadoopConfDir); conf != nil || err != nil {
			return conf, err
		}
	}

	hadoopHome := os.Getenv("HADOOP_HOME")
	if hadoopHome != "" {
		if conf, err := Load(filepath.Join(hadoopHome, "conf")); conf != nil || err != nil {
			return conf, err
		}
	}

	return nil, nil
}

// Load returns a HadoopConf object representing configuration from the
// specified path. It will parse core-site.xml, hdfs-site.xml, and
// mapred-site.xml.
//
// If no configuration files could be found, Load returns a nil map. If the
// configuration files exist but there was an error opening or parsing them,
// that is returned as well.
func Load(path string) (HadoopConf, error) {
	var conf HadoopConf

	for _, file := range confFiles {
		pList := propertyList{}
		f, err := ioutil.ReadFile(filepath.Join(path, file))
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return conf, err
		}

		err = xml.Unmarshal(f, &pList)
		if err != nil {
			return conf, fmt.Errorf("%s: %s", path, err)
		}

		if conf == nil {
			conf = make(HadoopConf)
		}

		for _, prop := range pList.Property {
			conf[prop.Name] = prop.Value
		}
	}

	return conf, nil
}

// DefaultNamenodes returns the namenodes that should be used given the
// configuration's fs.defaultFS (or deprecated fs.default.name) property. If no
// such property is found, i.e. if the configuration is using neither federated
// namespaces nor high availability, then the dfs.namenode.rpc-address property
// is returned if present. Otherwise, a nil slice is returned.
func (conf HadoopConf) DefaultNamenodes() []string {
	if fs, ok := conf["fs.defaultFS"]; ok {
		// check if default nameservice is defined
		fsurl, _ := url.Parse(fs)
		if fsurl == nil {
			return nil
		}
		return conf.Namenodes(fsurl.Host)
	} else if ns, ok := conf["fs.default.name"]; ok {
		// check if default nameservice is defined (through deprecated name)
		return conf.Namenodes(ns)
	} else if nn, ok := conf["dfs.namenode.rpc-address"]; ok {
		// non-HA and non-federated config; return single namenode
		return []string{nn}
	} else {
		// no namenodes found at all
		return nil
	}
}

// namenodesPerNS returns a mapping from clusters to the namenode(s) in those
// clusters.
func (conf HadoopConf) namenodesPerNS() map[string][]string {
	nns := make(map[string][]string)
	var clusterNames []string

	// this property is required for high availability and/or federation. if
	// it's not set, the configuration must be using a non-federated and non-HA
	// architecture. check if the property is defined before updating
	// clusterNames because strings.Split will return a non-empty slice given
	// an empty string, covering up the distinction between no dfs.nameservices
	// given and an empty dfs.nameservices.
	if nameservices, ok := conf["dfs.nameservices"]; ok {
		clusterNames = append(clusterNames, strings.Split(nameservices, ",")...)
	}

	// obtain logical namenode ids per nameservice
	for _, ns := range clusterNames {
		nnids, ha := conf["dfs.ha.namenodes."+ns]
		if !ha {
			// non-HA federated architecture
			if nn, ok := conf["dfs.namenode.rpc-address."+ns]; ok {
				nns[ns] = append(nns[ns], nn)
			}
		} else {
			// HA architecture
			for _, nnid := range strings.Split(nnids, ",") {
				if nn, ok := conf["dfs.namenode.rpc-address."+ns+"."+nnid]; ok {
					nns[ns] = append(nns[ns], nn)
				}
			}
			sort.Strings(nns[ns])
		}
	}

	return nns
}

// Namenodes returns the namenode hosts present in the configuration for the
// given nameservice. The returned slice will be sorted. If no namenode
// addresses can be found, Namenodes returns a nil slice.
func (conf HadoopConf) Namenodes(ns string) []string {
	return conf.namenodesPerNS()[ns]
}
