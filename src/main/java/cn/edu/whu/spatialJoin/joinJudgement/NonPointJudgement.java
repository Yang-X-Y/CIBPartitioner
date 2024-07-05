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

import cn.edu.whu.spatialJoin.JTS.GridGeometry;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.strtree.STRtree;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class NonPointJudgement<T extends Geometry,U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<SpatialIndex>, Iterator<T>, PartResultStatistic>, Serializable {

    /**
     * @see JudgementBase
     */
    public NonPointJudgement(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams) {
        super(considerBoundaryIntersection, dedupParams);
    }

    @Override
    public Iterator<PartResultStatistic> call(Iterator<SpatialIndex> indexIterator,Iterator<T> streamShapes)
            throws Exception {
        List<PartResultStatistic> result = new ArrayList<>();
        PartResultStatistic partResSta = new PartResultStatistic();
        partResSta.partId = TaskContext.getPartitionId();
        partResSta.partEnv = getDedupParams().getPartitionExtents(). get(partResSta.partId);
        if (!indexIterator.hasNext() || !streamShapes.hasNext()) {
            result.add(partResSta);
            return result.iterator();
        }

        initPartition();
        int queryItemsNum = 0;
        int toRefineNum = 0;
        int resultNum = 0;
        double refineCost = 0.0;
        long filterTime = 0L;
        long refineTime = 0L;
        int indexedItemsNum = 0;
        long start = System.currentTimeMillis();
        SpatialIndex treeIndex = indexIterator.next();
        if (treeIndex instanceof STRtree) {
            indexedItemsNum = ((STRtree) treeIndex).getItemsNum();
        }

        while (streamShapes.hasNext()) {
            T streamShape = streamShapes.next();
            List<Geometry> candidates;
            queryItemsNum++;
            long startFilter = System.currentTimeMillis();
            candidates = treeIndex.query(streamShape.getEnvelopeInternal());
            long endFilter = System.currentTimeMillis();
            filterTime+=endFilter-startFilter;

            for (Geometry candidate : candidates) {
                if (referencePointCheck(candidate,streamShape)){
                    refineCost += ((candidate.getNumPoints()+streamShape.getNumPoints())*Math.log(candidate.getNumPoints()+streamShape.getNumPoints()));
                    toRefineNum += 1;
                    if (geoMatch(candidate,streamShape)) {
                        resultNum+=1;
                    }
                }
            }
            long endRefine = System.currentTimeMillis();
            refineTime+=endRefine-endFilter;
        }
        long end = System.currentTimeMillis();
        double filterCost = (indexedItemsNum==0 || queryItemsNum==0) ? 0: (queryItemsNum)*Math.log(indexedItemsNum);
        partResSta.queryItemsNum = queryItemsNum;
        partResSta.indexedItemsNum = indexedItemsNum;
        partResSta.estimatedCost = estimatedCost;
        partResSta.filterCost = filterCost;
        partResSta.refineCost = refineCost;
        partResSta.toRefineNum = toRefineNum;
        partResSta.resultNum = resultNum;
        partResSta.filterTime = filterTime;
        partResSta.refineTime = refineTime;
        partResSta.joinTime = end-start;
        result.add(partResSta);
        return result.iterator();
    }
}
