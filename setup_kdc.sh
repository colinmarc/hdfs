#!/bin/sh

JAR_NAME="simple-kdc.jar"
SRV_CONF_DIR="server-conf"
KDC_HOME="/tmp/kdc-home"
KEYTAB_DIR="$KDC_HOME/admin-keytab"

if [ ! $KERBEROS = "true" ]; then
    echo "Kerberos disabled for this test: not starting KDC"
    exit 0
fi

echo "Initialising KDC and assorted keytabs."

rm -rf $KDC_HOME
rm /tmp/krb5cc_hdfs_test
mkdir -p $KEYTAB_DIR

cd test/kdc
# Initializes the KDC: creates an admin keytab, initializes the jsonbackend.son file
./kdcinit.sh $SRV_CONF_DIR $KEYTAB_DIR
# Start the KDC and let it run in the background
./start-kdc.sh $SRV_CONF_DIR $KDC_HOME &
# Wait for the KDC to start
sleep 3

# Generate the user and the service keytabs.
echo "Adding user principal and generating keytab..."
echo "addprinc -pw password $USER@EXAMPLE.COM\nktadd -k client.keytab $USER@EXAMPLE.COM\nexit\n" | ./kadmin.sh $SRV_CONF_DIR -k $KEYTAB_DIR/admin.keytab

echo "Adding service principal and generating keytab..."
echo "addprinc -randkey nn/localhost@EXAMPLE.COM\nktadd -k nn.keytab nn/localhost@EXAMPLE.COM\nexit\n" | ./kadmin.sh $SRV_CONF_DIR -k $KEYTAB_DIR/admin.keytab
echo "Moving the keytabs to $KDC_HOME"
mv client.keytab $KDC_HOME
mv nn.keytab $KDC_HOME
cp krb5.conf $KDC_HOME

# Initialize a credential cache.
./kinit.sh -conf $KDC_HOME -k -t $KDC_HOME/client.keytab  $USER@EXAMPLE.COM
# Move the credential cache to where the native client expects it
cp /tmp/krb5cc_hdfs_test /tmp/krb5cc_$(id -u $(whoami))
