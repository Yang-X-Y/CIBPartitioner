package cn.edu.whu.spatialJoin.spatialPartitioning;

import cn.edu.whu.spatialJoin.enums.PartitionerType;

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

    private String partitionerSize;

    private List samples = null;
    private GridHistogramCI gridHistogramCI = null;
    private Envelope boundaryEnvelope = null;

    public PartitionUtil(PartitionerType partitionType,int partitionNum, int cellNum, double alpha){
        this.partitionType = partitionType;
        this.partitionNum = partitionNum;
        this.cellNum = cellNum;
        this.alpha = alpha;
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
        if (partitionType.equals(PartitionerType.CIBPartitioner)){
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
        }  else {
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
            default:
                throw new Exception("[AbstractSpatialRDD][spatialPartitioning] Unsupported spatial partitioning method. " +
                        "The following partitioning methods are not longer supported: R-Tree, Hilbert curve, Voronoi, Equal-Grids");
        }
        partitionerSize = RamUsageEstimator.humanSizeOf(partitioner);

        return partitioner;
    }

    public String getPartitionerSize(){
        return this.partitionerSize;
    }

}
