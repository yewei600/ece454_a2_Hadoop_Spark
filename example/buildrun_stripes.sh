#!/bin/sh

#export JAVA_TOOL_OPTIONS=-Xmx1g
export JAVA_HOME=/usr/lib/jvm/jre-1.8.0
export HADOOP_HOME=/opt/hadoop-2.7.2
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

javac CCStripes.java
if [ $? -ne 0 ]; then
    exit
fi

jar cf ccs.jar CCStripes*.class

rm -fr output

INPUT_PREFIX=input
INPUT=$INPUT_PREFIX/Trudeau.txt

time $HADOOP_HOME/bin/hadoop jar ccs.jar CCStripes $INPUT output

cat output/part* | sort -t',' -n -k2 -r | head -n25
