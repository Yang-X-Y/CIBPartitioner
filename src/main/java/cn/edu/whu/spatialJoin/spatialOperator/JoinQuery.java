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

package cn.edu.whu.spatialJoin.spatialOperator;

import cn.edu.whu.spatialJoin.enums.IndexType;
import cn.edu.whu.spatialJoin.enums.JoinBuildSide;
import cn.edu.whu.spatialJoin.geometryObjects.Circle;
import cn.edu.whu.monitoring.Metric;
import cn.edu.whu.monitoring.Metrics;
import cn.edu.whu.spatialJoin.joinJudgement.*;
import cn.edu.whu.spatialJoin.rangeJudgement.JudgementBase;
import cn.edu.whu.spatialJoin.spatialPartitioning.SpatialPartitioner;
import cn.edu.whu.spatialJoin.spatialRDD.CircleRDD;
import cn.edu.whu.spatialJoin.spatialRDD.SpatialRDD;
import cn.edu.whu.spatialJoin.utils.GeomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class JoinQuery {
    private static final Logger log = LogManager.getLogger(JoinQuery.class);

    private static <U extends Geometry, T extends Geometry> void verifyCRSMatch(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD)
            throws Exception {
        // Check CRS information before doing calculation. The two input RDDs are supposed to have the same EPSG code if they require CRS transformation.
        if (spatialRDD.getCRStransformation() != queryRDD.getCRStransformation()) {
            throw new IllegalArgumentException("[JoinQuery] input RDD doesn't perform necessary CRS transformation. Please check your RDD constructors.");
        }

        if (spatialRDD.getCRStransformation() && queryRDD.getCRStransformation()) {
            if (!spatialRDD.getTargetEpgsgCode().equalsIgnoreCase(queryRDD.getTargetEpgsgCode())) {
                throw new IllegalArgumentException("[JoinQuery] the EPSG codes of two input RDDs are different. Please check your RDD constructors.");
            }
        }
    }

    private static <U extends Geometry, T extends Geometry> void verifyPartitioningMatch(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD)
            throws Exception {
        Objects.requireNonNull(spatialRDD.spatialPartitionedRDD, "[JoinQuery] spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
        Objects.requireNonNull(queryRDD.spatialPartitionedRDD, "[JoinQuery] queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");

        final SpatialPartitioner spatialPartitioner = spatialRDD.getPartitioner();
        final SpatialPartitioner queryPartitioner = queryRDD.getPartitioner();

        if (!queryPartitioner.equals(spatialPartitioner)) {
            throw new IllegalArgumentException("[JoinQuery] queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
        }

        final int spatialNumPart = spatialRDD.spatialPartitionedRDD.getNumPartitions();
        final int queryNumPart = queryRDD.spatialPartitionedRDD.getNumPartitions();
        if (spatialNumPart != queryNumPart) {
            throw new IllegalArgumentException("[JoinQuery] numbers of partitions in queryRDD and spatialRDD don't match: " + queryNumPart + " vs. " + spatialNumPart + ". Please make sure they both use the same partitioning otherwise wrong results will appear.");
        }
    }

    public static <U extends Geometry, T extends Geometry> JavaRDD<PartResultStatistic> SpatialJoinOnlySta(SpatialRDD<T> indexedRDD, SpatialRDD<U> queryRDD, boolean considerBoundaryIntersection,boolean isPIP)
            throws Exception {
        final JoinParams params = new JoinParams(true, considerBoundaryIntersection);
        return spatialJoinOnlyStatistic(indexedRDD, queryRDD, params,isPIP);
    }

    public static <U extends Geometry, T extends Geometry> JavaRDD<PartResultStatistic> spatialJoinOnlyStatistic(
            SpatialRDD<U> leftRDD,
            SpatialRDD<T> rightRDD,
            JoinParams joinParams,
            boolean isPIP)
            throws Exception {

        verifyCRSMatch(leftRDD, rightRDD);
        verifyPartitioningMatch(leftRDD, rightRDD);

        final SpatialPartitioner partitioner =
                (SpatialPartitioner) leftRDD.spatialPartitionedRDD.partitioner().get();
        final DedupParams dedupParams = partitioner.getDedupParams();

        final JavaRDD<PartResultStatistic> joinResult;

        if (isPIP){
            PIPJudgement judgement = new PIPJudgement(joinParams.considerBoundaryIntersection, dedupParams);
            joinResult = leftRDD.indexedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, judgement);
        } else{
            NonPointJudgement judgement = new NonPointJudgement(joinParams.considerBoundaryIntersection, dedupParams);
            joinResult = leftRDD.indexedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, judgement);
        }

        return joinResult;
    }


    public static final class JoinParams {
        public final boolean useIndex;
        public final boolean considerBoundaryIntersection;
        public final IndexType indexType;
        public final JoinBuildSide joinBuildSide;

        public JoinParams(boolean useIndex, boolean considerBoundaryIntersection) {
            this(useIndex, considerBoundaryIntersection, IndexType.RTREE, JoinBuildSide.RIGHT);
        }

        public JoinParams(boolean considerBoundaryIntersection, IndexType polygonIndexType, JoinBuildSide joinBuildSide) {
            this.useIndex = false;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.indexType = polygonIndexType;
            this.joinBuildSide = joinBuildSide;
        }

        public JoinParams(boolean useIndex, boolean considerBoundaryIntersection, IndexType polygonIndexType, JoinBuildSide joinBuildSide) {
            this.useIndex = useIndex;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.indexType = polygonIndexType;
            this.joinBuildSide = joinBuildSide;
        }
    }
}

