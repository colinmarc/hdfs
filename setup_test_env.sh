#!/bin/sh

HADOOP_DISTRO=${HADOOP_DISTRO-"cdh"}
HADOOP_HOME=${HADOOP_HOME-"/tmp/hadoop-$HADOOP_DISTRO"}
NN_PORT=${NN_PORT-"9000"}
HADOOP_NAMENODE="localhost:$NN_PORT"

if [ ! -d "$HADOOP_HOME" ]; then
  mkdir -p $HADOOP_HOME

  if [ $HADOOP_DISTRO = "cdh" ]; then
      HADOOP_URL="http://archive.cloudera.com/cdh5/cdh/5/hadoop-latest.tar.gz"
  elif [ $HADOOP_DISTRO = "hdp" ]; then
      HADOOP_URL="http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.0.6.0/tars/hadoop-2.2.0.2.0.6.0-76.tar.gz"
  else
      echo "No/bad HADOOP_DISTRO='${HADOOP_DISTRO}' specified"
      exit 1
  fi

  echo "Downloading Hadoop from $HADOOP_URL to ${HADOOP_HOME}/hadoop.tar.gz"
  curl -o ${HADOOP_HOME}/hadoop.tar.gz -L $HADOOP_URL

  echo "Extracting ${HADOOP_HOME}/hadoop.tar.gz into $HADOOP_HOME"
  tar zxf ${HADOOP_HOME}/hadoop.tar.gz --strip-components 1 -C $HADOOP_HOME
fi

MINICLUSTER_JAR=$(find $HADOOP_HOME -name "hadoop-mapreduce-client-jobclient*.jar" | grep -v tests | grep -v sources | head -1)
if [ ! -f "$MINICLUSTER_JAR" ]; then
  echo "Couldn't find minicluster jar"
  exit 1
fi
echo "minicluster jar found at $MINICLUSTER_JAR"


# start the namenode in the background
$HADOOP_HOME/bin/hadoop jar $MINICLUSTER_JAR minicluster -nnport $NN_PORT -nomr -format $@ &
sleep 10

echo "bar" > foo.txt
HADOOP_FS="$HADOOP_HOME/bin/hadoop fs -Ddfs.block.size=1048576"
$HADOOP_FS -put foo.txt "hdfs://$HADOOP_NAMENODE/"

curl -o mobydick.txt -L http://www.gutenberg.org/cache/epub/2701/pg2701.txt
$HADOOP_FS -put mobydick.txt "hdfs://$HADOOP_NAMENODE/"

$HADOOP_FS -mkdir "hdfs://$HADOOP_NAMENODE/empty"
$HADOOP_FS -mkdir "hdfs://$HADOOP_NAMENODE/full"
$HADOOP_FS -mkdir "hdfs://$HADOOP_NAMENODE/full/dir"
$HADOOP_FS -put foo.txt "hdfs://$HADOOP_NAMENODE/full/1"
$HADOOP_FS -put foo.txt "hdfs://$HADOOP_NAMENODE/full/2"
$HADOOP_FS -put foo.txt "hdfs://$HADOOP_NAMENODE/full/3"

$HADOOP_FS -mkdir "hdfs://$HADOOP_NAMENODE/todeletedir"
$HADOOP_FS -put foo.txt "hdfs://$HADOOP_NAMENODE/todelete"

$HADOOP_FS -put foo.txt "hdfs://$HADOOP_NAMENODE/tomove"
