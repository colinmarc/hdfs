set -e

hadoop fs -mkdir -p "/_test"
hadoop fs -chmod 777 "/_test"

if [ "$TRANSPARENT_ENCRYPTION" = "true" ]; then
    echo "Prepare encrypted zone"
    hadoop fs -mkdir /_test/kms
    hadoop fs -chmod 777 "/_test/kms"
    hadoop key create key1
    hdfs crypto -createZone -keyName key1 -path /_test/kms
fi

hadoop fs -put ./testdata/foo.txt "/_test/foo.txt"
hadoop fs -Ddfs.block.size=1048576 -put ./testdata/mobydick.txt "/_test/mobydick.txt"
