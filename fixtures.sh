set -e

HADOOP_FS=${HADOOP_FS-"hadoop fs"}
$HADOOP_FS -mkdir -p "hdfs://$HADOOP_NAMENODE/_test"
$HADOOP_FS -chmod 777 "hdfs://$HADOOP_NAMENODE/_test"

$HADOOP_FS -put ./testdata/foo.txt "hdfs://$HADOOP_NAMENODE/_test/foo.txt"
$HADOOP_FS -Ddfs.block.size=1048576 -put ./testdata/mobydick.txt "hdfs://$HADOOP_NAMENODE/_test/mobydick.txt"
