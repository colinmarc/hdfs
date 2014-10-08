#!/bin/sh

HADOOP_HOME=${HADOOP_HOME-"/usr/local/hadoop"}
NN_PORT=${NN_PORT-"9000"}
MINICLUSTER_JAR=$(find $HADOOP_HOME -name "hadoop-mapreduce-client-jobclient*.jar" | grep -v tests | head -1)

echo $MINICLUSTER_JAR
export HADOOP_NAMENODE="localhost:$NN_PORT"

exec $HADOOP_HOME/bin/hadoop jar $MINICLUSTER_JAR minicluster -nnport $NN_PORT -nomr -format $@
