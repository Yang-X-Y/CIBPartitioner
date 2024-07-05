package cn.edu.whu.spatialJoin.spatialPartitioning;

import cn.edu.whu.spatialJoin.enums.PartitionerType;
import cn.edu.whu.spatialJoin.spatialPartitioning.ADP.ADPartitioner;
import cn.edu.whu.spatialJoin.spatialPartitioning.ADP.ADPartitioning;
import cn.edu.whu.spatialJoin.spatialPartitioning.ADP.ADPStatistic;
import cn.edu.whu.spatialJoin.spatialPartitioning.ADP.GridHistogramADP;

import cn.edu.whu.spatialJoin.spatialPartitioning.KDB.KDBTreePartitioner;
import cn.edu.whu.spatialJoin.spatialPartitioning.KDB.KDBTreePartitioning;
import cn.edu.whu.spatialJoin.spatialPartitioning.equalGrid.EqualPartitioning;
import cn.edu.whu.spatialJoin.spatialPartitioning.equalGrid.FlatGridPartitioner;
import cn.edu.whu.spatialJoin.spatialPartitioning.CIBP.*;
import cn.edu.whu.spatialJoin.spatialPartitioning.quadtree.QuadTreePartitioner;
import cn.edu.whu.spatialJoin.spatialPartitioning.quadtree.QuadtreePartitioning;
import cn.edu.whu.spatialJoin.spatialPartitioning.voronoi.VoronoiPartitioner;
import cn.edu.whu.spatialJoin.spatialPartitioning.voronoi.VoronoiPartitioning;
import cn.edu.whu.spatialJoin.spatialRDD.SpatialRDD;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class PartitionUtil<T extends Geometry> implements Serializable {

    private PartitionerType partitionType;
    private int partitionNum;
    private int cellNum;
    private double alpha;
    private double indexDataSampleRate;
    private double queryDataSampleRate;

    private String partitionerSize;

    private List samples = null;
    private GridHistogramCI gridHistogramCI = null;
    private GridHistogramADP gridHistogramADP = null;
    private Envelope boundaryEnvelope = null;

    public PartitionUtil(PartitionerType partitionType,int partitionNum, int cellNum, double alpha, double indexDataSampleRate, double queryDataSampleRate){
        this.partitionType = partitionType;
        this.partitionNum = partitionNum;
        this.cellNum = cellNum;
        this.alpha = alpha;
        this.indexDataSampleRate=indexDataSampleRate;
        this.queryDataSampleRate=queryDataSampleRate;
    }

    public List defaultSample(SpatialRDD<T> rdd) {
        List samples = rdd.rawSpatialRDD.sample(false, 0.01)
                .map(new Function<T, Object>() {
                    @Override
                    public Object call(T geometry)
                            throws Exception {
                        return geometry.getEnvelopeInternal();
                    }
                })
                .collect();
        return samples;
    }

    public List<Tuple2> sampleNonPoint(SpatialRDD<T> rdd, double sampleRate) {
        List<Tuple2> samples;
        if (sampleRate>=1) {
            samples = rdd.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, Tuple2>() {
                @Override
                public Iterator<Tuple2> call(final Iterator<T> geoms) {
                    List<Tuple2> list = new ArrayList<>();
                    while (geoms.hasNext()){
                        T geom = geoms.next();
                        list.add(new Tuple2(geom.getEnvelopeInternal(),geom.getNumPoints()));
                    }
                    return list.iterator();
                }
            }).collect();
        } else {
            samples = rdd.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, Tuple2>() {
                @Override
                public Iterator<Tuple2> call(final Iterator<T> geoms) {
                    List<Tuple2> list = new ArrayList<>();
                    Random random = new Random();
                    while (geoms.hasNext()){
                        T geom = geoms.next();
                        if (random.nextDouble()<sampleRate) {
                            list.add(new Tuple2(geom.getEnvelopeInternal(),geom.getNumPoints()));
                        }
                    }
                    return list.iterator();
                }
            }).collect();
        }

        return samples;
    }

    public List<Coordinate> samplePoint(SpatialRDD<T> rdd, double sampleRate) {
        List<Coordinate> samples;
        if (sampleRate>=1) {
            samples = rdd.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, Coordinate>() {
                @Override
                public Iterator<Coordinate> call(final Iterator<T> geoms) {
                    List<Coordinate> list = new ArrayList<>();
                    while (geoms.hasNext()){
                        T geom = geoms.next();
                        list.add(geom.getCoordinate());
                    }
                    return list.iterator();
                }
            }).collect();
        } else {
            samples = rdd.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, Coordinate>() {
                @Override
                public Iterator<Coordinate> call(final Iterator<T> geoms) {
                    List<Coordinate> list = new ArrayList<>();
                    Random random = new Random();
                    while (geoms.hasNext()){
                        T geom = geoms.next();
                        if (random.nextDouble()<sampleRate) {
                            list.add(geom.getCoordinate());
                        }
                    }
                    return list.iterator();
                }
            }).collect();
        }

        return samples;
    }

    public GHCIStatistic gridStatistic(SpatialRDD<T> rdd, boolean isPoint) {
//        JavaRDD<T> samples = rdd.rawSpatialRDD.sample(false, 0.01);

        GHCIStatistic rddStatistic = rdd.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, GHCIStatistic>() {
            @Override
            public Iterator<GHCIStatistic> call(Iterator<T> geomIterator) {
                GHCIStatistic gridHistogramStatistic = new GHCIStatistic(cellNum,isPoint);
                List<GHCIStatistic> list = new ArrayList<>();
                while (geomIterator.hasNext()) {
                    gridHistogramStatistic.stat(geomIterator.next());
                }
                list.add(gridHistogramStatistic);
                return list.iterator();
            }
        }).reduce((value1, value2) -> value1.combine(value2));
        return rddStatistic;
    }

    public ADPStatistic PIPCIStatistic(SpatialRDD<T> indexRdd, SpatialRDD<T> queryRdd, Boolean indexRddIsPoint){
        // computational intensity
        double gridCostSum = 0.0;
        int indexDataSampleNums;

        ADPStatistic computationalIntensityStatistic = new ADPStatistic(boundaryEnvelope,cellNum);

        List<Coordinate> samplePoints;
        List<Tuple2> sampleNonPoints;
        if (indexRddIsPoint) {
            samplePoints = samplePoint(indexRdd, indexDataSampleRate);
            sampleNonPoints = sampleNonPoint(queryRdd,queryDataSampleRate);
            indexDataSampleNums = samplePoints.size();
        } else {
            samplePoints = samplePoint(queryRdd,queryDataSampleRate);
            sampleNonPoints = sampleNonPoint(indexRdd,indexDataSampleRate);
            indexDataSampleNums = sampleNonPoints.size();
        }
        double costFiltering = (1/(queryDataSampleRate))*Math.log(indexDataSampleNums/(indexDataSampleRate*partitionNum))*(1-alpha);
        double refinementExpander = (1/(indexDataSampleRate*queryDataSampleRate))*alpha;
        String samplePointsSize = RamUsageEstimator.humanSizeOf(samplePoints);
        String sampleNonPointsSize = RamUsageEstimator.humanSizeOf(sampleNonPoints);
        System.out.println("[mylog]indexRdd:"+indexRdd.rawSpatialRDD.count()+" indexDataSampleRate: "+indexDataSampleRate+" sample: "+(indexRddIsPoint?samplePoints.size():sampleNonPoints.size())+" samplePointsSize:"+(indexRddIsPoint?samplePointsSize:sampleNonPointsSize));
        System.out.println("[mylog]queryRdd:"+queryRdd.rawSpatialRDD.count()+" queryDataSampleRate: "+queryDataSampleRate+" sample: "+(indexRddIsPoint?sampleNonPoints.size():samplePoints.size())+" sampleNonPointsSize:"+(indexRddIsPoint?sampleNonPointsSize:samplePointsSize));

        STRtree strTree = new STRtree();
        for (Tuple2 s:sampleNonPoints) {
            Envelope env = (Envelope) s._1;
            Coordinate centre = env.centre();
            if (!indexRddIsPoint && boundaryEnvelope.contains(centre)) {
                computationalIntensityStatistic.stat(centre,costFiltering);
                gridCostSum+=costFiltering;
            }
            strTree.insert(env,s);
        }
        for (Coordinate r: samplePoints){
            if (indexRddIsPoint && boundaryEnvelope.contains(r)) {
                computationalIntensityStatistic.stat(r,costFiltering);
                gridCostSum+=costFiltering;
            }
            Envelope rEnv = new Envelope(r);
            List<Tuple2> results = strTree.query(rEnv);
            for (Tuple2 res: results){
                double costRefinement = refinementExpander*((Integer)res._2);
                gridCostSum+=costRefinement;
                computationalIntensityStatistic.stat(r,costRefinement);
            }
        }

        String strTreeSize = RamUsageEstimator.humanSizeOf(strTree);
        System.out.println("[mylog]strTreeSize:"+strTreeSize);

        System.gc();
        return computationalIntensityStatistic;
    }

    public ADPStatistic nonPointCIStatistic(SpatialRDD<T> indexRdd, SpatialRDD<T> queryRdd){
        // computational intensity
        List<Tuple2> indexDataSample = sampleNonPoint(indexRdd,indexDataSampleRate);
        List<Tuple2> queryDataSample = sampleNonPoint(queryRdd,queryDataSampleRate);
        String indexDataSampleSize = RamUsageEstimator.humanSizeOf(indexDataSample);
        String queryDataSampleSize = RamUsageEstimator.humanSizeOf(queryDataSample);
        int indexDataSampleNums = indexDataSample.size();
        System.out.println("[mylog]indexRdd:"+indexRdd.rawSpatialRDD.count()+" indexDataSampleRate: "+indexDataSampleRate+" indexDataSampleNum: "+indexDataSample.size()+" indexDataSampleSize:"+indexDataSampleSize);
        System.out.println("[mylog]queryRdd:"+queryRdd.rawSpatialRDD.count()+" queryDataSampleRate: "+queryDataSampleRate+" queryDataSampleNum: "+queryDataSample.size()+" queryDataSampleSize:"+queryDataSampleSize);
        double costFiltering = (1/queryDataSampleRate)*Math.log(indexDataSampleNums/(indexDataSampleRate*partitionNum))*(1-alpha);
        double refinementExpander = (1/(indexDataSampleRate*queryDataSampleRate))*alpha;
        ADPStatistic computationalIntensityStatistic = new ADPStatistic(boundaryEnvelope,cellNum);
        STRtree strTree = new STRtree();
        for (Tuple2 s1:indexDataSample) {
            Envelope env = (Envelope) s1._1;
            strTree.insert(env,s1);
//            gridHistogramStatistic2.stat((Envelope) s1._1);
        }
        for (Tuple2 s2: queryDataSample){
            Envelope env = (Envelope) s2._1;
            Coordinate centre = env.centre();
            if (boundaryEnvelope.contains(centre)) {
                computationalIntensityStatistic.stat(centre,costFiltering);
            }
//            gridHistogramStatistic2.stat((Envelope) s2._1);
            List<Tuple2> results = strTree.query((Envelope) s2._1);
            for (Tuple2 r: results){
                Envelope commonMBR = ((Envelope) r._1).intersection((Envelope) s2._1);
                if (commonMBR!=null) {
//                    System.out.println(r);
//                    System.out.println(s2);
                    double cost = refinementExpander*((Integer)s2._2+(Integer)r._2)*Math.log((Integer)s2._2+(Integer)r._2);
//                    double cost = 1/Math.pow(sampleRate,2)*((Integer)s2._2+(Integer)r._2);
                    computationalIntensityStatistic.stat(commonMBR,cost);
                }
            }
        }
        String strTreeSize = RamUsageEstimator.humanSizeOf(strTree);
        System.out.println("[mylog]strTreeSize:"+strTreeSize);
        System.gc();
        return computationalIntensityStatistic;
    }

//    public GridHistogramCountStatistic gridCount(SpatialRDD<T> rdd) {
////        JavaRDD<T> samples = rdd.rawSpatialRDD.sample(false, 0.01);
//
//        JavaRDD<GridHistogramCountStatistic> a = rdd.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, GridHistogramCountStatistic>() {
//            @Override
//            public Iterator<GridHistogramCountStatistic> call(Iterator<T> geomIterator) {
//                GridHistogramCountStatistic gridHistogramCountStatistic = new GridHistogramCountStatistic(cellNum);
//                List<GridHistogramCountStatistic> list = new ArrayList<>();
//                while (geomIterator.hasNext()) {
//                    gridHistogramCountStatistic.stat(geomIterator.next());
//                }
//                list.add(gridHistogramCountStatistic);
//                return list.iterator();
//            }
//        });
//        GridHistogramCountStatistic rddStatistic = a.reduce((value1, value2) -> value1.combine(value2));
//        return rddStatistic;
//    }

//    public List sample(SpatialRDD<T> indexRdd, SpatialRDD<T> queryRdd){
//
//        List samples = new ArrayList<>();
//        List samples1 = sample(indexRdd);
//        List samples2 = sample(queryRdd);
//        samples.addAll(samples1);
//        samples.addAll(samples2);
//        return samples;
//    }

    public Envelope computeBoundary(SpatialRDD<T> indexRdd, SpatialRDD<T> queryRdd){

        indexRdd.analyze();
        queryRdd.analyze();
        Envelope env1 = indexRdd.boundaryEnvelope;
        Envelope env2 = queryRdd.boundaryEnvelope;
        Envelope intersectEnv = env1.intersection(env2);
        return intersectEnv;
    }

    public Envelope computeBoundary(SpatialRDD<T> indexRdd){
        indexRdd.analyze();
        Envelope env1 = indexRdd.boundaryEnvelope;
        final Envelope paddedBoundary = new Envelope(
                env1.getMinX(), env1.getMaxX() + 0.01,
                env1.getMinY(), env1.getMaxY() + 0.01);
        return paddedBoundary;
    }

    public void analysis(SpatialRDD<T> indexRdd, SpatialRDD<T> queryRdd)
            throws Exception {
        if (partitionType.equals(PartitionerType.ADP)) {
            boundaryEnvelope = computeBoundary(indexRdd, queryRdd);
            ADPStatistic ADPStatistic;
            boolean indexRddIsPoint = (indexRdd.rawSpatialRDD.first() instanceof Point);
            boolean queryRddIsPoint = (queryRdd.rawSpatialRDD.first() instanceof Point);
            if (indexRddIsPoint || queryRddIsPoint) {
                ADPStatistic = PIPCIStatistic(indexRdd,queryRdd,indexRddIsPoint);
            } else {
                ADPStatistic = nonPointCIStatistic(indexRdd,queryRdd);
            }

            gridHistogramADP = ADPStatistic.computeCost();
        } else if (partitionType.equals(PartitionerType.CIBPartitioner)){
            boolean indexRddIsPoint = (indexRdd.rawSpatialRDD.first() instanceof Point);
            boolean queryRddIsPoint = (queryRdd.rawSpatialRDD.first() instanceof Point);
            GHCIStatistic indexGHCI = gridStatistic(indexRdd,indexRddIsPoint);
            long indexSize = indexRdd.rawSpatialRDD.count();
            double indexLength = Math.log((double) indexSize/partitionNum);
            GHCIStatistic queryGHCI = gridStatistic(queryRdd,queryRddIsPoint);
            if (indexRddIsPoint || queryRddIsPoint) {
                if (queryRddIsPoint) {
                    gridHistogramCI = indexGHCI.computePIPCI(queryGHCI,alpha,indexLength);
                } else {
                    gridHistogramCI = queryGHCI.computePIPCI(indexGHCI,alpha,indexLength);
                }
            } else {
                gridHistogramCI = indexGHCI.computeCost(queryGHCI,alpha,indexLength);
            }
//            boundaryEnvelope = new Envelope(-180.0,180.0,-90.0,90.0);
//            CIStatistic computationalIntensityStatistic = nonPointCIStatistic(indexRdd, queryRdd);
//            double[][] trueCost = computationalIntensityStatistic.getHistogramCost();
//            double[][] GHCost = gridHistogramCost.getHistogramCost();
//            int h = trueCost.length;
//            int w = trueCost[0].length;
//            List<Double> trueCostList = new ArrayList<>();
//            List<Double> GHCostList = new ArrayList<>();
//            for(int i=0;i<h;i++) {
//                for (int j=0;j<w;j++){
//                    if (trueCost[i][j]>0) {
//                        trueCostList.add(trueCost[i][j]);
//                        GHCostList.add(GHCost[i][j]);
//                    }
//                }
//            }
//            double pccs = DataStatisticsUtils.getPearsonCorrelationScore(trueCostList, GHCostList);
////            System.out.println("GHCostSum:"+gridHistogramCost.getCostSum());
////            System.out.println("trueCostSum:"+computationalIntensityStatistic.getCostSum());
//            System.out.println("[mylog] pccs:"+pccs);
        }  else {
//            boundaryEnvelope = computeBoundary(indexRdd,queryRdd);
//            samples = sample(indexRdd, queryRdd);
            boundaryEnvelope = computeBoundary(indexRdd);
            samples = defaultSample(indexRdd);
        }
    }

    public SpatialPartitioner buildPartitioner()
            throws Exception {

        SpatialPartitioner partitioner;
        switch (partitionType) {

            case CIBPartitioner: {
                CIBPartitioning CIBPartitioning = new CIBPartitioning(gridHistogramCI,partitionNum);
                partitioner = new CIBPartitioner(CIBPartitioning.getPartitionTree().getRoot());
                break;
            }
            case ADP: {
                ADPartitioning ADPartitioning = new ADPartitioning(gridHistogramADP,cellNum,partitionNum,boundaryEnvelope);
                partitioner = new ADPartitioner(ADPartitioning.getPartitionTree().getRoot());
                break;
            }
            case EQUALGRID: {
                EqualPartitioning equalPartitioning = new EqualPartitioning(boundaryEnvelope, partitionNum);
                partitioner = new FlatGridPartitioner(equalPartitioning.getGrids());
                break;
            }
            case QUADTREE: {
                QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning((List<Envelope>) samples, boundaryEnvelope, partitionNum);
                partitioner = new QuadTreePartitioner(quadtreePartitioning.getPartitionTree());
                break;
            }
            case KDBTREE: {
                KDBTreePartitioning kdbTreePartitioning = new KDBTreePartitioning(samples, boundaryEnvelope, partitionNum);
                partitioner = new KDBTreePartitioner(kdbTreePartitioning.getPartitionTree());
                break;
            }
            case VORONOI: {
                VoronoiPartitioning voronoiPartitioning = new VoronoiPartitioning(samples, partitionNum);
                partitioner = new VoronoiPartitioner(voronoiPartitioning);
                break;
            }
//            case EQUALGRID: {
//                // Force the quad-tree to grow up to a certain level
//                // So the actual num of partitions might be slightly different
//                int minLevel = (int) Math.max(Math.log(partitionNum)/Math.log(4), 0);
//                QuadtreePartitioning quadtreePartitioning = null;
//                try {
//                    quadtreePartitioning = new QuadtreePartitioning(new ArrayList<>(), boundaryEnvelope
//                            , partitionNum, minLevel);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                partitioner = new QuadTreePartitioner(quadtreePartitioning.getPartitionTree());
//                break;
//            }
//            case GRIDQUADTREE: {
//                cn.edu.whu.spatialJoin.spatialPartitioning.GridQuadTreePartitionerNew.GridQuadTreePartitioning gridQuadTreePartitioning = new GridQuadTreePartitioning((List<Envelope>) samples, boundaryEnvelope, partitionNum);
//                partitioner = new GridQuadTreePartitioner(gridQuadTreePartitioning.getPartitionTree());
//                break;
//            }
//            case GRIDKDBTREE: {
//                GridKDBTreePartitioning gridKDBTreePartitioning = new GridKDBTreePartitioning(samples, boundaryEnvelope, partitionNum,cellNum);
//                partitioner = new GridKDBTreePartitioner(gridKDBTreePartitioning.getPartitionTree().getRoot());
//                break;
//            }
//            case GRIDCOUNTKDBTREE: {
//                GridCountKDBTreePartitioning gridCountKDBTreePartitioning = new GridCountKDBTreePartitioning(gridHistogramCount,partitionNum);
//                partitioner = new GridCountKDBTreePartitioner(gridCountKDBTreePartitioning.getPartitionTree().getRoot());
//                break;
//            }
            default:
                throw new Exception("[AbstractSpatialRDD][spatialPartitioning] Unsupported spatial partitioning method. " +
                        "The following partitioning methods are not longer supported: R-Tree, Hilbert curve, Voronoi, Equal-Grids");
        }
        partitionerSize = RamUsageEstimator.humanSizeOf(partitioner);

//        StringBuilder a = new StringBuilder("GEOMETRYCOLLECTION(");
//        GeometryFactory gf = new GeometryFactory();
//        for (Envelope env: partitioner.grids) {
//            a.append(gf.toGeometry(env)).append(",");
//        }
//        a.append(")");
//        System.out.println(a.toString());
        return partitioner;
    }

    public String getPartitionerSize(){
        return this.partitionerSize;
    }

}
