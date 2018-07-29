package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/colinmarc/hdfs"
)

var cachedClients map[string]*hdfs.Client = make(map[string]*hdfs.Client)

func getClient(namenode string) (*hdfs.Client, error) {
	if cachedClients[namenode] != nil {
		return cachedClients[namenode], nil
	}

	if namenode == "" {
		namenode = os.Getenv("HADOOP_NAMENODE")
	}

	// Ignore errors here, since we don't care if the conf doesn't exist if the
	// namenode was specified.
	conf := hdfs.LoadHadoopConf("")
	options, _ := hdfs.ClientOptionsFromConf(conf)
	if namenode != "" {
		options.Addresses = []string{namenode}
	}

	if options.Addresses == nil {
		return nil, errors.New("couldn't find a namenode to connect to. You should specify hdfs://<namenode>:<port> in your paths. Alternatively, set HADOOP_NAMENODE or HADOOP_CONF_DIR in your environment")
	}

	var err error
	options.User = os.Getenv("HADOOP_USER_NAME")
	if options.User == "" {
		options.User, err = hdfs.Username()
		if err != nil {
			return nil, fmt.Errorf("couldn't determine user: %s", err)
		}
	}

	c, err := hdfs.NewClient(options)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to namenode: %s", err)
	}

	cachedClients[namenode] = c
	return c, nil
}

func formatBytes(i uint64) string {
	switch {
	case i > (1024 * 1024 * 1024 * 1024):
		return fmt.Sprintf("%#.1fT", float64(i)/1024/1024/1024/1024)
	case i > (1024 * 1024 * 1024):
		return fmt.Sprintf("%#.1fG", float64(i)/1024/1024/1024)
	case i > (1024 * 1024):
		return fmt.Sprintf("%#.1fM", float64(i)/1024/1024)
	case i > 1024:
		return fmt.Sprintf("%#.1fK", float64(i)/1024)
	default:
		return fmt.Sprintf("%dB", i)
	}
}
