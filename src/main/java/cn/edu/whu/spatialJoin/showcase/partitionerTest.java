/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.whu.spatialJoin.showcase;

import cn.edu.whu.spatialJoin.enums.FileDataSplitter;
import cn.edu.whu.spatialJoin.enums.IndexType;
import cn.edu.whu.spatialJoin.enums.PartitionerType;
import cn.edu.whu.spatialJoin.joinJudgement.PartResultStatistic;
import cn.edu.whu.spatialJoin.spatialOperator.JoinQuery;
import cn.edu.whu.spatialJoin.spatialPartitioning.PartitionUtil;
import cn.edu.whu.spatialJoin.spatialPartitioning.SpatialPartitioner;
import cn.edu.whu.spatialJoin.spatialRDD.LineStringRDD;
import cn.edu.whu.spatialJoin.spatialRDD.PointRDD;
import cn.edu.whu.spatialJoin.spatialRDD.PolygonRDD;
import cn.edu.whu.spatialJoin.spatialRDD.SpatialRDD;
import cn.edu.whu.spatialJoin.utils.DataStatisticsUtils;
import cn.edu.whu.spatialJoin.utils.JsonUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.index.strtree.STRtree;
import org.parboiled.common.Tuple2;
import scala.Tuple4;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;


/**
 * The Class Example.
 */
public class partitionerTest
        implements Serializable {

    /**
     * The sc.
     */
    public static JavaSparkContext sc = null;

    /**
     * dataset location.
     */
    static String dataPath;
    static String poisPath;
    static String parksPath;
    static String lakesPath;
    static String buildingsPath;
    static String roadsPath;

    /**
     * partition number.
     */
    static int partitionNum;

    /**
     * cell number.
     */
    static int cellNum;

    static double alpha;

    static String indexDataType;

    static String queryDataType;

    static String spatialPredicate;

    static String localIndex;

    static boolean considerBoundaryIntersection=true;
    static boolean isPIP=false;
    static String partitionerStr;
    static PartitionerType partitionerType;
    static IndexType localIndexType;

    /**
     * dataset splitter.
     */
    static FileDataSplitter splitter = FileDataSplitter.WKT;

    static SpatialRDD indexRDD;
    static SpatialRDD queryRDD;

    /**
     * statistic result.
     */
    static long loadTime;
    static long analysisTime;
    static long partitionTime;
    static long indexTime;
    static long cacheTime;
    static long joinTime;
    static long totalTime;
    static int resultNum;
    static double joinTimeCV;
    static String filePath;
    static String partitionerSize;

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args) throws Exception {

//        String jsonFile = args[0];
//
        String jsonFile = args[0];
        int loadParts=256;
        JSONObject jsonConf = JsonUtil.readLocalJSONFile(jsonFile);
        initConfig(jsonConf);
        initIndexes();
        if (STRtree.class.getDeclaredMethod("getItemsNum")==null) throw new Exception("[mylog] strTree doesn't has getItemsNum method");

        // load data
        long startLoadT = System.currentTimeMillis();
        indexRDD = loadData(indexDataType,loadParts);
        queryRDD = loadData(queryDataType,loadParts);
        long endLoadT = System.currentTimeMillis();
        loadTime = endLoadT-startLoadT;

        // spatial Partition
        PartitionUtil partitionUtil = new PartitionUtil(partitionerType,partitionNum,cellNum,alpha);
        partitionUtil.analysis(indexRDD,queryRDD);
//        System.exit(0);
        long endAnalysisT = System.currentTimeMillis();
        analysisTime = endAnalysisT-endLoadT;

        SpatialPartitioner partitioner = partitionUtil.buildPartitioner();
        partitionerSize = partitionUtil.getPartitionerSize();
        indexRDD.spatialPartitioning(partitioner);
        queryRDD.spatialPartitioning(partitioner);
        System.out.println("[mylog]indexPartitionedRDD:"+indexRDD.spatialPartitionedRDD.first());
        System.out.println("[mylog]queryPartitionedRDD:"+queryRDD.spatialPartitionedRDD.first());
        long endPartitionT = System.currentTimeMillis();
        partitionTime = endPartitionT-endAnalysisT;

        // build local index;
        indexRDD.buildIndex(localIndexType, true);
        System.out.println("[mylog]indexedRDD:"+indexRDD.indexedRDD.first());
        long endIndexT = System.currentTimeMillis();
        indexTime = endIndexT - endPartitionT;

        long endCacheT = System.currentTimeMillis();
        cacheTime = endCacheT-endIndexT;

        // spatialJoin
        // long resultSize = JoinQuery.SpatialJoinQueryFlat(indexRDD, queryRDD, true, considerBoundaryIntersection).count();

        //spatialJoinOnlyStatistic
        List result = JoinQuery.SpatialJoinOnlySta(indexRDD, queryRDD, considerBoundaryIntersection,isPIP).collect();
        long endJoinT = System.currentTimeMillis();
        joinTime = endJoinT-endCacheT;
        totalTime = endJoinT-startLoadT;

        sc.stop();

        resultStatistic(result);
    }

    private static void buildSparkContext(boolean local) {

        if (sc != null)
            return ;
        SparkConf conf = new SparkConf();
        conf.setAppName("join test");
        conf.set("spark.serializer", KryoSerializer.class.getName());
//        conf.set("spark.kryo.registrator", StdeKryoRegistrator.class.getName());
        if (local){
            conf.setMaster("local[*]");
        }
        sc = new JavaSparkContext(conf);

    }

    public static void initConfig(JSONObject jsonConf){

        buildSparkContext(false);
        dataPath = jsonConf.getString("dataPath");
        poisPath = dataPath+"osm21_pois.csv";
        roadsPath = dataPath+"roads.csv";
        parksPath = dataPath+"parks_polygon.csv";
        lakesPath = dataPath+"lakes_polygon.csv";
        buildingsPath = dataPath+"buildings.csv";
        indexDataType = jsonConf.getString("indexDataType");
        queryDataType = jsonConf.getString("queryDataType");
        spatialPredicate = jsonConf.getString("spatialPredicate");
        cellNum = jsonConf.getIntValue("cellNum");
        partitionNum = jsonConf.getIntValue("partitionNum");
        partitionerStr = jsonConf.getString("partitioner");
        localIndex = jsonConf.getString("localIndex");
        alpha = jsonConf.getDouble("alpha");
        if (spatialPredicate.equals("contain")){ considerBoundaryIntersection = false; }
    }

    public static void initIndexes(){
        switch (partitionerStr) {
            case "equalGrid":
                partitionerType = PartitionerType.EQUALGRID;
                break;
            case "quadTree":
                partitionerType = PartitionerType.QUADTREE;
                break;
            case "KDBTree":
                partitionerType = PartitionerType.KDBTREE;
                break;
            case "voronoi":
                partitionerType = PartitionerType.VORONOI;
                break;
            case "CIBP":
                partitionerType = PartitionerType.CIBPartitioner;
                break;
        }
        switch (localIndex) {
            case "quadTree":
                localIndexType = IndexType.QUADTREE;
                break;
            case "RTree":
                localIndexType = IndexType.RTREE;
                break;
        }
    }
    public static SpatialRDD loadData(String dataType, int loadParts){
        SpatialRDD dataRDD = null;
        switch (dataType) {
            case "pois":
                isPIP = true;
                dataRDD = new PointRDD(sc, poisPath, null, splitter, true, loadParts);
                break;
            case "roads":
                dataRDD = new LineStringRDD(sc, roadsPath, null, null, splitter, true, loadParts);
                break;
            case "parks":
                dataRDD = new PolygonRDD(sc, parksPath, null, null, splitter, true, loadParts);
                break;
            case "lakes":
                dataRDD = new PolygonRDD(sc, lakesPath, null, null, splitter, true, loadParts);
                break;
            case "buildings":
                dataRDD = new PolygonRDD(sc, buildingsPath, null, null, splitter, true, loadParts);
                break;
            default:
                System.out.println("unsupported dataTpe: "+dataType);
                break;
        }
        return dataRDD;
    }

    public static void resultStatistic(List results)
            throws Exception {
        List<Double> trueCosts = new ArrayList<>();
        List<Double> estimatedCosts = new ArrayList<>();
        List<Double> joinTimes = new ArrayList<>();
        for (Object result:results) {
            PartResultStatistic r = (PartResultStatistic) result;
            double partFilterCost =  r.filterCost;
            double partRefineCost = r.refineCost;
            double partTrueCost=((1-alpha)*partFilterCost+alpha*partRefineCost);
            trueCosts.add(partTrueCost);
            estimatedCosts.add(r.estimatedCost);
            joinTimes.add((double) r.joinTime);
            resultNum += r.resultNum;
        }

        joinTimeCV = DataStatisticsUtils.getCoefficientVariation(joinTimes);
        double r_CI_JT = DataStatisticsUtils.getPearsonCorrelationScore(trueCosts,joinTimes);
        double r_ECI_JT = DataStatisticsUtils.getPearsonCorrelationScore(estimatedCosts,joinTimes);
        double r_CI_ECI = DataStatisticsUtils.getPearsonCorrelationScore(trueCosts,estimatedCosts);

        String resultStr = ("************************ resultStatistic ************************"
                + "\nindexDataType: " + indexDataType
                + "\nqueryDataType: " + queryDataType
                + "\npartitionNum: " + joinTimes.size()
                + "\npartitionerType: " + partitionerType
                + "\nlocalIndexType: " + localIndexType
                + "\npartitionerSize: "+ partitionerSize
                + "\ncellNum: " + cellNum
                + "\nalpha: " + alpha
                + "\nresultNum:" + resultNum
                + "\ntotalTime: " + totalTime + "ms"
                + "\nloadTime: " + loadTime + "ms"
                + "\nanalysisTime: " + analysisTime + "ms"
                + "\npartitionTime: " + partitionTime + "ms"
                + "\nindexTime: " + indexTime + "ms"
                + "\ncacheTime: " + cacheTime + "ms"
                + "\njoinTime: " + joinTime + "ms"
                + "\njoinTimeCV: " + joinTimeCV
                + "\nr_CI_JT: " + r_CI_JT
                + "\nr_ECI_JT: " + r_ECI_JT
                + "\nr_CI_ECI: " + r_CI_ECI);
        System.out.println(resultStr);


    }
}