package cn.edu.whu.spatialJoin.spatialRDD;

import cn.edu.whu.spatialJoin.JTS.GridPoint;
import cn.edu.whu.spatialJoin.enums.FileDataSplitter;
import cn.edu.whu.spatialJoin.formatMapper.FormatMapper;
import cn.edu.whu.spatialJoin.formatMapper.GridPointFormatMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Envelope;

public class GridPointRDD extends SpatialRDD<GridPoint> {

    public GridPointRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, int level) {

        JavaRDD rawTextRDD = partitions != null ? sparkContext.textFile(InputLocation, partitions) : sparkContext.textFile(InputLocation);
        this.setRawSpatialRDD(rawTextRDD.mapPartitions(new GridPointFormatMapper(level)));

    }

}
