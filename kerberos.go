package hdfs

import (
	"log"
	"os"
	"os/user"
	"strings"

	"gopkg.in/jcmturner/gokrb5.v4/client"
	"gopkg.in/jcmturner/gokrb5.v4/config"
	"gopkg.in/jcmturner/gokrb5.v4/credentials"
	"gopkg.in/jcmturner/gokrb5.v4/keytab"
)

const HdfsDefaultServiceName = "nn"
const KrbDefaultCfgPath = "/etc/krb5.conf"
const HadoopAuthCfgPath = "hadoop.security.authentication"

// GetKrbClientIfRequired returns a kerberos client if the hadoop
// configuration or the environment variables suggest one is required
func GetKrbClientIfRequired(conf HadoopConf) *client.Client {
	// Check if the kerberos config path has been overriden
	var krb5Cfg = os.Getenv("HADOOP_KRB_CONF")

	if krb5Cfg == "" {
		krb5Cfg = KrbDefaultCfgPath
	}

	// Now check if the credential cache or the keytab have been manually specified
	keytabPath := os.Getenv("HADOOP_KEYTAB")

	if keytabPath != "" {
		return getKrbClientWithKeytab(krb5Cfg, keytabPath)
	}

	credCachePath := os.Getenv("HADOOP_CCACHE")
	if credCachePath != "" {
		return getKrbClientWithCredCache(krb5Cfg, credCachePath)
	}

	// At this point none of the required env vars have been required
	// and we need to check the configuration to know if kerberos is enabled
	val, found := conf[HadoopAuthCfgPath]
	if !found || "kerberos" != strings.ToLower(val) {
		return nil
	}

	// Kerberos is enabled. fall back to the default credential cache location.
	// TODO: read the kerberos config to determine where the cred cache is located?
	return getKrbClientWithCredCache(krb5Cfg, getDefaultCredCachePath())
}

// returns "/tmp/krb5cc_$(id -u $(whoami))"
func getDefaultCredCachePath() string {
	u, e := user.Current()
	if e != nil {
		log.Panic(e)
	}
	return "/tmp/krb5cc_" + u.Uid
}

func getKrbClientWithCredCache(configPath string, cachePath string) *client.Client {
	cfg, cfgE := config.Load(configPath)

	if cfgE != nil {
		log.Panic(cfgE)
	}

	cc, cce := credentials.LoadCCache(cachePath)

	if cce != nil {
		log.Panic(cce)
	}

	cl, clE := client.NewClientFromCCache(cc)
	if clE != nil {
		log.Panic(clE)
	}

	cl.WithConfig(cfg)
	// TODO Config flag or whatever for people not using AD
	cl.GoKrb5Conf.DisablePAFXFast = true

	return &cl

}

func getKrbClientWithKeytab(configPath string, keytabPath string) *client.Client {

	cfg, cfgE := config.Load(configPath)

	if cfgE != nil {
		log.Panic(cfgE)
	}

	kt, ktE := keytab.Load(keytabPath)

	if ktE != nil {
		log.Panic(ktE)
	}

	entries := kt.Entries

	if len(entries) == 0 {
		log.Fatalf("no entries found in keytab %s" + keytabPath)
	}

	// Fetch the principal of the first entry
	principal := entries[0].Principal

	cl := client.NewClientWithKeytab(strings.Join(principal.Components, "/"), principal.Realm, kt)
	cl.WithConfig(cfg)

	// TODO Config flag or whatever for people not using AD
	cl.GoKrb5Conf.DisablePAFXFast = true
	if loginE := cl.Login(); loginE != nil {
		log.Panic(loginE)
	}
	return &cl
}

// GetServiceName returns 'nn' unless the HADOOP_SNAME environment variable is set
func GetServiceName() string {
	if sn := os.Getenv("HADOOP_SNAME"); sn != "" {
		return sn
	}
	return HdfsDefaultServiceName
}
