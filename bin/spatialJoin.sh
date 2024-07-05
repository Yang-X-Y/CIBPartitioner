#!/bin/bash

function spatialJoin(){
  spark-submit \
    --master spark://huawei5:7077 \
    --class cn.edu.whu.spatialJoin.showcase.ExampleSpatialJoin \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --executor-cores 5 \
    --num-executors 24 \
    --conf spark.driver.memory=4g \
    --conf spark.executor.memory=10g \
    --conf spark.driver.extraClassPath="/opt/module/spark-2.4.4/HBaseJar/*:/opt/module/hadoop-2.7.7/etc/hadoop/:/opt/module/hbase-1.4.9/conf/" \
    --conf spark.executor.extraClassPath="/opt/module/spark-2.4.4/HBaseJar/*:/opt/module/hadoop-2.7.7/etc/hadoop/:/opt/module/hbase-1.4.9/conf/" \
    /home/yxy/spatialJoin/grid-based-spatial-join-1.0-SNAPSHOT.jar "${joinMode}" "240" "20"
}

joinMode="geospark"

eval spatialJoin
