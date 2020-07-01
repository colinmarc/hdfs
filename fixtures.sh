set -e

# jdk11 is missing some APIs that the older jars here rely on
# so point at openjdk8 for now
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

HADOOP_FS=${HADOOP_FS-"hadoop fs"}
$HADOOP_FS -mkdir -p "/_test"
$HADOOP_FS -chmod 777 "/_test"

$HADOOP_FS -put ./testdata/foo.txt "/_test/foo.txt"
$HADOOP_FS -Ddfs.block.size=1048576 -put ./testdata/mobydick.txt "/_test/mobydick.txt"
