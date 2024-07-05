package cn.edu.whu.spatialJoin.spatialRDD;

import cn.edu.whu.spatialJoin.JTS.GridLineString;
import cn.edu.whu.spatialJoin.formatMapper.GridLineStringFormatMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class GridLineStringRDD extends SpatialRDD<GridLineString> {

    public GridLineStringRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions,int SEG, boolean isFixedLevel) {
        JavaRDD rawTextRDD = partitions != null ? sparkContext.textFile(InputLocation, partitions) : sparkContext.textFile(InputLocation);
        this.setRawSpatialRDD(rawTextRDD.mapPartitions(new GridLineStringFormatMapper(SEG,isFixedLevel)));
    }

}
