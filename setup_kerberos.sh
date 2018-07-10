#!/bin/sh
REALM_NAME="EXAMPLE.COM"
MASTER_PASS="master_password"
HOSTNAME=$(hostname)

if [ ! $KERBEROS = "true" ]; then
    echo "Kerberos disabled for this test: So, not configuring KDC"
    exit 0
fi

echo "Configuring hosts for kerberos"
sudo -- sh -c -e "echo -e '\n127.0.0.1 ${HOSTNAME} localhost' >> /etc/hosts";

echo "Installing kerberos server";
sudo apt-get update
sudo apt-get install -y krb5-user krb5-kdc krb5-admin-server

echo "Configuring kerberos server"
sudo cp ./test/kerberos/krb5.conf /etc/krb5.conf
sudo cp ./test/kerberos/kdc.conf /etc/krb5kdc/kdc.conf
sudo -- sh -c -e "echo -e '*/admin@EXAMPLE.COM     *' >> /etc/krb5kdc/kadm5.acl";

echo "Creating master database"
sudo kdb5_util -r ${REALM_NAME} create -s << EOL
${MASTER_PASS}
${MASTER_PASS}
EOL

echo "Creating namenode principal"
sudo kadmin.local -q "addprinc -randkey nn/localhost@${REALM_NAME}"
sudo kadmin.local -q "addprinc -randkey nn/${HOSTNAME}@${REALM_NAME}"

echo "Creating client principal"
sudo kadmin.local -q "addprinc -randkey hdfs/localhost@${REALM_NAME}"
sudo kadmin.local -q "addprinc -randkey hdfs/${HOSTNAME}@${REALM_NAME}"

echo "Creating namenode keytab file"
sudo kadmin.local -q "xst -k /tmp/nn.keytab nn/localhost@${REALM_NAME}"
sudo kadmin.local -q "xst -k /tmp/nn.keytab nn/${HOSTNAME}@${REALM_NAME}"

echo "Creating client keytab file"
sudo kadmin.local -q "xst -k /tmp/client.keytab hdfs/localhost@${REALM_NAME}"
sudo kadmin.local -q "xst -k /tmp/client.keytab hdfs/${HOSTNAME}@${REALM_NAME}"

echo "Starting kerberos server"
sudo service krb5-kdc restart
sudo service krb5-admin-server restart

sudo chmod +rx /tmp/nn.keytab
sudo chmod +rx /tmp/client.keytab

echo "Kerberos logging in"
kinit -kt /tmp/client.keytab hdfs/localhost@${REALM_NAME}