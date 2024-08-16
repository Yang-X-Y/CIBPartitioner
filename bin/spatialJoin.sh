#!/bin/bash

function spatialJoin(){
  spark-submit \
    --master spark://**:7077 \
    --class cn.edu.whu.spatialJoin.showcase.partitionerTest \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --executor-cores 4 \
    --num-executors 75 \
    --conf spark.driver.memory=5g \
    --conf spark.executor.memory=10g \
    --conf spark.driver.maxResultSize=60g \
    --conf spark.kryoserializer.buffer.max=1g \
    ~/CIBPartitioner-1.0-SNAPSHOT.jar "${config}"
}

config=$1

eval spatialJoin
