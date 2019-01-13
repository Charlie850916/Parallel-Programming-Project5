#!bin/bash -xe
JAR=PageRank.jar

hdfs dfs -rm -r ans.out
hadoop jar $JAR pageRank.PageRank hdfs:///user/ta/PageRank/input-$1 $2
hdfs dfs -get ans.out
mv ans.out pagerank_$1.out
