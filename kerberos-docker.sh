#!/bin/bash

set -e

IP_ADDRESS=$(hostname -I)
HOSTNAME=$(cat /etc/hostname)

export KERBEROS_PASSWORD=password
export KERBEROS_REALM=example.com
export KERBEROS_HOSTNAME=$HOSTNAME.$KERBEROS_REALM


echo "Configure the hosts file for Kerberos to work in a container"
cp /etc/hosts ~/hosts.new
sed -i "/.*$HOSTNAME/c\\$IP_ADDRESS\t$KERBEROS_HOSTNAME" ~/hosts.new
cp -f ~/hosts.new /etc/hosts

echo "Setting up Kerberos config file at /etc/krb5.conf"
cat > /etc/krb5.conf << EOL
[libdefaults]
    default_realm = EXAMPLE.COM
    dns_lookup_realm = false
    dns_lookup_kdc = false
[realms]
    EXAMPLE.COM = {
        kdc = $KERBEROS_HOSTNAME
        admin_server = $KERBEROS_HOSTNAME
    }
[domain_realm]
    .$HOSTNAME = EXAMPLE.COM
    $HOSTNAME = EXAMPLE.COM
[logging]
    kdc = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmin.log
    default = FILE:/var/log/krb5lib.log
EOL

mkdir /etc/krb5kdc
echo -e "*/*@EXAMPLE.COM\t*" > /etc/krb5kdc/kadm5.acl

apt-get update
apt-get \
    -y \
    -qq \
    install \
    krb5-{user,kdc,admin-server}


# krb5_newrealm returns non-0 return code as it is running in a container, ignore it for this command only
set +e
printf "$KERBEROS_PASSWORD\n$KERBEROS_PASSWORD" | krb5_newrealm
set -e

echo "Creating principals for tests"
rm -f client.keytab
kadmin.local -q "addprinc -randkey $TEST_USER@EXAMPLE.COM"
kadmin.local -q "ktadd -k client.keytab $TEST_USER"

rm -f nn.keytab
kadmin.local -q "addprinc -randkey nn/localhost@EXAMPLE.COM"
kadmin.local -q "ktadd -k nn.keytab nn/localhost"

echo "Restarting Kerberos KDS service"
service krb5-kdc restart

rm -f cred_cache
kinit -V -kt client.keytab $TEST_USER
cp /tmp/krb5cc_0 cred_cache

tail -f /var/log/krb5kdc.log
