#!/bin/sh

KDC_HOME="/tmp/kdc-home"

if [ ! $KERBEROS = "true" ]; then
    echo "Kerberos disabled for this test: not starting KDC"
    exit 0
fi

echo "Initialising KDC and assorted keytabs."

rm -rf $KDC_HOME

mkdir -p $KDC_HOME

cp test/kdc/krb5.conf $KDC_HOME

docker run -d -v $(pwd):$(pwd) -w $(pwd) -e TEST_USER=$USER -p 5088:88 ubuntu:16.04 /bin/bash kerberos-docker.sh
sleep 30

mv client.keytab $KDC_HOME
mv nn.keytab $KDC_HOME
mv cred_cache /tmp/krb5cc_$(id -u $(whoami))
