#!/bin/sh

set -e

KERBEROS="${KERBEROS-false}"
AES=${AES-"false"}
if [ "$DATA_TRANSFER_PROTECTION" = "privacy" ]; then
  KERBEROS="true"
  ENCRYPT_DATA_TRANSFER="true"
  ENCRYPT_DATA_TRANSFER_ALG="rc4"
  if [ "$AES" = "true" ]; then
      ENCRYPT_DATA_TRANSFER_CIPHER="AES/CTR/NoPadding"
  fi
else
  ENCRYPT_DATA_TRANSFER="false"
fi

CONF_KMS_PROVIDER=""
TRANSPARENT_ENCRYPTION=false
if [ "$HADOOP_VERSION" != "2.10.1" ]; then
    TRANSPARENT_ENCRYPTION=true
    CONF_KMS_PROVIDER="kms://http@localhost:9600/kms"
fi

CONF_AUTHENTICATION="simple"
KERBEROS_REALM="EXAMPLE.COM"
KERBEROS_PRINCIPLE="administrator"
KERBEROS_PASSWORD="password1234"
if [ "$KERBEROS" = "true" ]; then
  CONF_AUTHENTICATION="kerberos"

  HOSTNAME=$(hostname)

  sudo tee /etc/krb5.conf << EOF
[libdefaults]
    default_realm = $KERBEROS_REALM
    dns_lookup_realm = false
    dns_lookup_kdc = false
[realms]
    $KERBEROS_REALM = {
        kdc = localhost
        admin_server = localhost
    }
[logging]
    default = FILE:/var/log/krb5libs.log
    kdc = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmind.log
[domain_realm]
    .localhost = $KERBEROS_REALM
    localhost = $KERBEROS_REALM
EOF

  sudo mkdir /etc/krb5kdc
  sudo printf '*/*@%s\t*' "$KERBEROS_REALM" | sudo tee /etc/krb5kdc/kadm5.acl

  sudo apt-get update
  sudo apt-get install -y krb5-user krb5-kdc krb5-admin-server

  printf "$KERBEROS_PASSWORD\n$KERBEROS_PASSWORD" | sudo kdb5_util -r "$KERBEROS_REALM" create -s
  for p in nn dn kms $USER gohdfs1 gohdfs2; do
    sudo kadmin.local -q "addprinc -randkey $p/$HOSTNAME@$KERBEROS_REALM"
    sudo kadmin.local -q "addprinc -randkey $p/localhost@$KERBEROS_REALM"
    sudo kadmin.local -q "xst -k /tmp/$p.keytab $p/$HOSTNAME@$KERBEROS_REALM"
    sudo kadmin.local -q "xst -k /tmp/$p.keytab $p/localhost@$KERBEROS_REALM"
    sudo chmod +rx /tmp/$p.keytab
  done
  # HTTP service for KMS
  sudo kadmin.local -q "addprinc -randkey HTTP/localhost@$KERBEROS_REALM"
  sudo kadmin.local -q "xst -k /tmp/kms.keytab HTTP/localhost@$KERBEROS_REALM"

  echo "Restarting krb services..."
  sudo service krb5-kdc restart
  sudo service krb5-admin-server restart

  kinit -kt /tmp/$USER.keytab "$USER/localhost@$KERBEROS_REALM"

  # The go tests need ccache files for these principles in a specific place.
  for p in $USER gohdfs1 gohdfs2; do
    kinit -kt "/tmp/$p.keytab" -c "/tmp/krb5cc_gohdfs_$p" "$p/localhost@$KERBEROS_REALM"
  done
fi

URL="https://dlcdn.apache.org/hadoop/core/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
echo "Downloading $URL"
curl -o hadoop.tar.gz $URL
tar zxf hadoop.tar.gz

HADOOP_ROOT="hadoop-${HADOOP_VERSION}/"
mkdir -p /tmp/hdfs/name /tmp/hdfs/data

sudo tee $HADOOP_ROOT/etc/hadoop/core-site.xml <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.security.authentication</name>
    <value>$CONF_AUTHENTICATION</value>
  </property>
  <property>
    <name>hadoop.security.authorization</name>
    <value>$KERBEROS</value>
  </property>
  <property>
    <name>dfs.namenode.keytab.file</name>
    <value>/tmp/nn.keytab</value>
  </property>
  <property>
    <name>dfs.namenode.kerberos.principal</name>
    <value>nn/localhost@$KERBEROS_REALM</value>
  </property>
  <property>
    <name>dfs.web.authentication.kerberos.principal</name>
    <value>nn/localhost@$KERBEROS_REALM</value>
  </property>
  <property>
    <name>dfs.datanode.keytab.file</name>
    <value>/tmp/dn.keytab</value>
  </property>
  <property>
    <name>dfs.datanode.kerberos.principal</name>
    <value>dn/localhost@$KERBEROS_REALM</value>
  </property>
  <property>
    <name>hadoop.rpc.protection</name>
    <value>$RPC_PROTECTION</value>
  </property>
  <property>
    <name>hadoop.security.key.provider.path</name>
    <value>$CONF_KMS_PROVIDER</value>
  </property>
</configuration>
EOF

sudo tee $HADOOP_ROOT/etc/hadoop/hdfs-site.xml <<EOF
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/tmp/hdfs/name</value>
  </property>
  <property>
    <name>dfs.namenode.fs-limits.min-block-size</name>
    <value>131072</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/tmp/hdfs/data</value>
  </property>
  <property>
   <name>dfs.permissions.superusergroup</name>
   <value>hadoop</value>
  </property>
  <property>
    <name>dfs.safemode.extension</name>
    <value>0</value>
  </property>
  <property>
     <name>dfs.safemode.min.datanodes</name>
     <value>1</value>
  </property>
  <property>
    <name>dfs.block.access.token.enable</name>
    <value>$KERBEROS</value>
  </property>
  <property>
    <name>ignore.secure.ports.for.testing</name>
    <value>true</value>
  </property>
  <property>
    <name>dfs.data.transfer.protection</name>
    <value>$DATA_TRANSFER_PROTECTION</value>
  </property>
  <property>
    <name>dfs.encrypt.data.transfer</name>
    <value>$ENCRYPT_DATA_TRANSFER</value>
  </property>
  <property>
    <name>dfs.encrypt.data.transfer.algorithm</name>
    <value>$ENCRYPT_DATA_TRANSFER_ALG</value>
  </property>
  <property>
    <name>dfs.encrypt.data.transfer.cipher.suites</name>
    <value>$ENCRYPT_DATA_TRANSFER_CIPHER</value>
  </property>
</configuration>
EOF

$HADOOP_ROOT/bin/hdfs namenode -format
sudo groupadd hadoop
sudo usermod -a -G hadoop $USER

sudo tee $HADOOP_ROOT/etc/hadoop/kms-site.xml <<EOF
<configuration>
  <property>
    <name>hadoop.kms.key.provider.uri</name>
    <value>jceks://file@/tmp/hdfs/kms.keystore</value>
  </property>
  <property>
    <name>hadoop.security.keystore.java-keystore-provider.password-file</name>
    <value>kms.keystore.password</value>
  </property>
  <property>
    <name>hadoop.kms.authentication.type</name>
    <value>$CONF_AUTHENTICATION</value>
  </property>
  <property>
    <name>hadoop.kms.authentication.kerberos.keytab</name>
    <value>/tmp/kms.keytab</value>
  </property>
  <property>
    <name>hadoop.kms.authentication.kerberos.principal</name>
    <value>HTTP/localhost@$KERBEROS_REALM</value>
  </property>
</configuration>
EOF

sudo tee $HADOOP_ROOT/etc/hadoop/kms.keystore.password <<EOF
123456
EOF

if [ "$TRANSPARENT_ENCRYPTION" = "true" ]; then
    echo "Starting KMS..."
    rm $HADOOP_ROOT/etc/hadoop/kms-log4j.properties
    $HADOOP_ROOT/bin/hadoop kms > /tmp/hdfs/kms.log 2>&1 &
fi

echo "Starting namenode..."
$HADOOP_ROOT/bin/hdfs namenode > /tmp/hdfs/namenode.log 2>&1 &

echo "Starting datanode..."
$HADOOP_ROOT/bin/hdfs datanode > /tmp/hdfs/datanode.log 2>&1 &

sleep 5

echo "Waiting for cluster to exit safe mode..."
$HADOOP_ROOT/bin/hdfs dfsadmin -safemode wait

echo "HADOOP_CONF_DIR=$(pwd)/$HADOOP_ROOT/etc/hadoop" >> $GITHUB_ENV
echo "TRANSPARENT_ENCRYPTION=$TRANSPARENT_ENCRYPTION" >> $GITHUB_ENV
echo "$(pwd)/$HADOOP_ROOT/bin" >> $GITHUB_PATH
