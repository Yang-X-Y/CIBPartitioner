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
        indexDataType = args[1].split(";")[0];
        queryDataType = args[1].split(";")[1];
        partitionerStr = args[2].split(";")[0];
        partitionNum = Integer.parseInt(args[2].split(";")[1]);
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
        List<Double> itemsNum = new ArrayList<>();
        List<Double> filterCost = new ArrayList<>();
        List<Double> refineCost = new ArrayList<>();
        List<Double> trueCosts = new ArrayList<>();
        double trueCostSum = 0.0;
        List<Double> estimatedCosts = new ArrayList<>();
        double estimatedCostsSum = 0.0;
        List<Double> filterTime = new ArrayList<>();
        List<Double> toRefineNum = new ArrayList<>();
        List<Double> refineTime = new ArrayList<>();
        List<Double> joinTimes = new ArrayList<>();
        List<Tuple4> partResults = new ArrayList<>();
        List<Tuple2> SEResults = new ArrayList<>();
        for (Object result:results) {
            PartResultStatistic r = (PartResultStatistic) result;
            itemsNum.add((double) (r.indexedItemsNum+ r.queryItemsNum));
            double partFilterCost =  r.filterCost;
            double partRefineCost = r.refineCost;
            filterCost.add(partFilterCost);
            refineCost.add(partRefineCost);
            double partTrueCost=((1-alpha)*partFilterCost+alpha*partRefineCost);
            trueCosts.add(partTrueCost);
            trueCostSum+=partTrueCost;
            estimatedCosts.add(r.estimatedCost);
            estimatedCostsSum+=(r.estimatedCost);
            filterTime.add((double) r.filterTime);
            refineTime.add((double) r.refineTime);
            toRefineNum.add((double) r.toRefineNum);
            joinTimes.add((double) r.joinTime);
            resultNum += r.resultNum;
            partResults.add(new Tuple4<>(r.indexedItemsNum+r.queryItemsNum,r.filterCost,r.refineCost,r.joinTime));
            SEResults.add(new Tuple2(partTrueCost,r.estimatedCost));
        }

        double itemsNumCV = DataStatisticsUtils.getCoefficientVariation(itemsNum);
        double estimatedCostCV = DataStatisticsUtils.getCoefficientVariation(estimatedCosts);
        double trueCostCV = DataStatisticsUtils.getCoefficientVariation(trueCosts);
        joinTimeCV = DataStatisticsUtils.getCoefficientVariation(joinTimes);
        double r_CI_JT = DataStatisticsUtils.getPearsonCorrelationScore(trueCosts,joinTimes);
        double r_ECI_JT = DataStatisticsUtils.getPearsonCorrelationScore(estimatedCosts,joinTimes);
        double r_N_JT = DataStatisticsUtils.getPearsonCorrelationScore(itemsNum,joinTimes);
        double r_Nc_JT = DataStatisticsUtils.getPearsonCorrelationScore(toRefineNum,joinTimes);
        double r_CI_ECI = DataStatisticsUtils.getPearsonCorrelationScore(trueCosts,estimatedCosts);
        double r_CI_N = DataStatisticsUtils.getPearsonCorrelationScore(trueCosts,itemsNum);
        double r_CI_Nc = DataStatisticsUtils.getPearsonCorrelationScore(trueCosts,toRefineNum);
        double r_ECI_N = DataStatisticsUtils.getPearsonCorrelationScore(estimatedCosts,itemsNum);
        double r_ECI_Nc = DataStatisticsUtils.getPearsonCorrelationScore(estimatedCosts,toRefineNum);
        double r_N_Nc = DataStatisticsUtils.getPearsonCorrelationScore(itemsNum,toRefineNum);
        double filteringPCCs = DataStatisticsUtils.getPearsonCorrelationScore(filterCost,filterTime);
        double refinementPCCs = DataStatisticsUtils.getPearsonCorrelationScore(refineCost,refineTime);

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
                + "\nestimatedCostCV: " + estimatedCostCV
                + "\ntrueCostCV: " + trueCostCV
                + "\nitemsNumCV: " + itemsNumCV
                + "\nr_CI_JT: " + r_CI_JT
                + "\nr_ECI_JT: " + r_ECI_JT
                + "\nr_N_JT: " + r_N_JT
                + "\nr_Nc_JT: " + r_Nc_JT
                + "\nr_CI_ECI: " + r_CI_ECI
                + "\nr_CI_N: " + r_CI_N
                + "\nr_CI_Nc: " + r_CI_Nc
                + "\nr_ECI_N: " + r_ECI_N
                + "\nr_ECI_Nc: " + r_ECI_Nc
                + "\nr_N_Nc: " + r_N_Nc
                + "\nfilteringPCCs: " + filteringPCCs
                + "\nrefinementPCCs: " + refinementPCCs
                + "\ntrueCostSum: " + trueCostSum
                + "\nestimatedCostsSum: " + estimatedCostsSum
                + "\nitemsNum\tfilterCost\trefineCost\tjoinTime\n " + partResults.stream().map(i->i._1()+"\t"+i._2()+"\t"+i._3()+"\t"+i._4()).collect(Collectors.joining("\n")));
//                + "\njoinTimes:\n" + joinTimes.stream().map(i->i.toString()).collect(Collectors.joining("\n"))
//                + "\nresultNum\titems\tjoinTime\n " + partResults.stream().map(i->i.a+"\t"+i.b+"\t"+i.c).collect(Collectors.joining("\n")));
//                + "\ntrueCost\testimateCost: " + SEResults.stream().map(i->i.a+"\t"+i.b).collect(Collectors.joining("\n"))
//                + "\njoinTimes:\n" + joinTimes.stream().map(i->i.toString()).collect(Collectors.joining("\n")));
        System.out.println(resultStr);
//        System.out.println("************trueCostJoinTimePCCs**********");
//        costTimePCCsList.forEach(i->System.out.println(i.a+"\t"+i.b));
//        SEResults.forEach(i->System.out.println(i.a+"\t"+i.b));
//        double costTimePCCs = DataStatisticsUtils.getPearsonCorrelationScore(refineCost,estimatedCosts);
//        double refinePCCs = DataStatisticsUtils.getPearsonCorrelationScore(refineCost,toRefineNum);
//        double refinePCCs2 = DataStatisticsUtils.getPearsonCorrelationScore(refineCost,refineTime);

//        System.out.println(costTimePCCs);
//        System.out.println(refinePCCs);
//        System.out.println(refinePCCs2);

        SimpleDateFormat dataFormat = new SimpleDateFormat("MMddHHmm");
        String timestamp = dataFormat.format(new Date());
        if (partitionerStr.equals("CIBPartitioner")) {
            filePath = "/home/yxy/CIBPartitioner/resultICDE/"+indexDataType+"-"+queryDataType+"-"+partitionerStr+"-"+localIndex+"-"+cellNum+"-"+partitionNum+"-"+timestamp+".txt";
        } else {
            filePath = "/home/yxy/CIBPartitioner/resultICDE/"+indexDataType+"-"+queryDataType+"-"+partitionerStr+"-"+localIndex+"-"+timestamp+".txt";
        }
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath))) {
            bufferedWriter.write(resultStr);
        }

    }
}