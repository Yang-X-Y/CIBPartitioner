package cn.edu.whu.spatialJoin.spatialRDD;

import cn.edu.whu.spatialJoin.JTS.GridPolygon;
import cn.edu.whu.spatialJoin.enums.FileDataSplitter;
import cn.edu.whu.spatialJoin.formatMapper.FormatMapper;
import cn.edu.whu.spatialJoin.formatMapper.GridPolygonFormatMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

import java.util.ArrayList;
import java.util.Arrays;

public class GridPolygonRDD extends SpatialRDD<GridPolygon> {

    public GridPolygonRDD (JavaSparkContext sparkContext, String InputLocation, Integer partitions, int SEG) {
        JavaRDD rawTextRDD = partitions != null ? sparkContext.textFile(InputLocation, partitions) : sparkContext.textFile(InputLocation);
        this.setRawSpatialRDD(rawTextRDD.mapPartitions(new GridPolygonFormatMapper(SEG)));
    }
}
