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

package cn.edu.whu.spatialJoin.joinJudgement;

import cn.edu.whu.spatialJoin.utils.HalfOpenRectangle;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.locationtech.jts.geom.*;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

/**
 * Base class for partition level join implementations.
 * <p>
 * Provides `match` method to test whether a given pair of geometries satisfies join condition.
 * <p>
 * Supports 'contains' and 'intersects' join conditions.
 * <p>
 * Provides optional de-dup logic. Due to the nature of spatial partitioning, the same pair of
 * geometries may appear in multiple partitions. If that pair satisfies join condition, it
 * will be included in join results multiple times. This duplication can be avoided by
 * (1) choosing spatial partitioning that doesn't allow for overlapping partition extents
 * and (2) reporting a pair of matching geometries only from the partition
 * whose extent contains the reference point of the intersection of the geometries.
 * <p>
 * To achieve (1), call SpatialRDD.spatialPartitioning with a GridType.QUADTREE. At the moment
 * this is the only grid type supported by de-dup logic.
 * <p>
 * For (2), provide `DedupParams` when instantiating JudgementBase object. If `DedupParams`
 * is specified, the implementation of the `match` method assumes that condition (1) holds.
 */
abstract class JudgementBase
        implements Serializable {
    private static final Logger log = LogManager.getLogger(JudgementBase.class);
    protected final DedupParams dedupParams;
    private final boolean considerBoundaryIntersection;
    String indexTable;
    String queryTable;
    double estimatedCost;
    transient private HalfOpenRectangle extent;

    /**
     * @param considerBoundaryIntersection true for 'intersects', false for 'contains' join condition
     * @param dedupParams                  Optional information to activate de-dup logic
     */
    protected JudgementBase(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams) {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
        this.dedupParams = dedupParams;
    }

    public JudgementBase(boolean considerBoundaryIntersection, DedupParams dedupParams, String indexTable, String queryTable) {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
        this.dedupParams = dedupParams;
        this.indexTable = indexTable;
        this.queryTable = queryTable;
    }

    /**
     * Looks up the extent of the current partition. If found, `match` method will
     * activate the logic to avoid emitting duplicate join results from multiple partitions.
     * <p>
     * Must be called before processing a partition. Must be called from the
     * same instance that will be used to process the partition.
     */
    protected void initPartition() {
        if (dedupParams == null) {
            return;
        }

        final int partitionId = TaskContext.getPartitionId();

        final List<Envelope> partitionExtents = dedupParams.getPartitionExtents();
        final List<Double> estimatedCosts = dedupParams.getEstimatedCosts();
        if (partitionId < partitionExtents.size()) {
            extent = new HalfOpenRectangle(partitionExtents.get(partitionId));
            if (estimatedCosts!=null) {
                estimatedCost = estimatedCosts.get(partitionId);
            }
        } else {
            log.warn("Didn't find partition extent for this partition: " + partitionId);
        }
    }

    protected boolean referencePointCheck(Geometry left, Geometry right) {
        if (left instanceof Point || right instanceof Point) {
            return true;
        }
        Envelope intersection =
                left.getEnvelopeInternal().intersection(right.getEnvelopeInternal());
        if (!intersection.isNull()) {
            final Point referencePoint =
                    makePoint(intersection.getMinX(), intersection.getMinY(), left.getFactory());
            return extent == null || extent.contains(referencePoint);
        }
        return false;
    }

    protected boolean match(Geometry left, Geometry right) {

        //System.out.println(left + "  " + right);
        if (extent != null) {

            /*if(left instanceof GridPolygon && right instanceof GridPoint){
                return GridPointInPolygon.contain((GridPolygon)left,(GridPoint)right);
            }else if(left instanceof GridPoint && right instanceof GridPolygon){
                return GridPointInPolygon.contain((GridPolygon)right,(GridPoint)left);
            }*/

            // Handle easy case: points. Since each point is assigned to exactly one partition,
            // different partitions cannot emit duplicate results.
            if (left instanceof Point || right instanceof Point) {
                return geoMatch(left, right);
            }

            // Neither geometry is a point

            // Check if reference point of the intersection of the bounding boxes lies within
            // the extent of this partition. If not, don't run any checks. Let the partition
            // that contains the reference point do all the work.
//            if (!referencePointCheck(left, right)) return false;
            Envelope intersection =
                    left.getEnvelopeInternal().intersection(right.getEnvelopeInternal());
            if (!intersection.isNull()) {
                final Point referencePoint =
                        makePoint(intersection.getMinX(), intersection.getMinY(), left.getFactory());
                if (!extent.contains(referencePoint)) {
                    return false;
                }
            }
        }

        return geoMatch(left, right);
    }

    private Point makePoint(double x, double y, GeometryFactory factory) {
        return factory.createPoint(new Coordinate(x, y));
    }

    protected boolean geoMatch(Geometry left, Geometry right) {
        //log.warn("Check "+left.toText()+" with "+right.toText());
        /*if(!considerBoundaryIntersection && left instanceof GridPolygon && right instanceof GridPoint) {
            return GridPointInPolygon.contain((GridPolygon) left, (GridPoint) right);
        }
        if (considerBoundaryIntersection){
            if (left instanceof GridLineString && right instanceof GridLineString)
                return  GridLineIntersection2.intersection((GridLineString)left, (GridLineString)right);
            if (left instanceof GridPolygon && right instanceof GridPolygon)
                return GridPolygonIntersection.intersection((GridPolygon)left, (GridPolygon)right);
        }*/

        /*if (!considerBoundaryIntersection && left instanceof Polygon && right instanceof Point){
            return GridPointInPolygon.contain(GridPolygon.transformPolygon((Polygon) left), GridPoint.transformPoint((Point)right));
        }
        if (considerBoundaryIntersection){
            if (left instanceof LineString && right instanceof LineString){
                return  GridLineIntersection2.intersection(GridLineString.transformLineString((LineString) left),
                        GridLineString.transformLineString((LineString)right));
            }

            if (left instanceof Polygon && right instanceof Polygon)
                return GridPolygonIntersection.intersection(GridPolygon.transformPolygon((Polygon)left),
                        GridPolygon.transformPolygon((Polygon)right));
        }*/
        if (considerBoundaryIntersection) {
            return left.intersects(right);
//            if (left.isValid() && right.isValid()) {
//                return left.intersects(right);
//            } else {
//                return false;
//            }
        } else {
            return (left instanceof Point) ? right.covers(left) : left.covers(right);
        }
//        return considerBoundaryIntersection ? left.intersects(right) : left.covers(right);
    }

    public DedupParams getDedupParams() {
        return dedupParams;
    }
}
