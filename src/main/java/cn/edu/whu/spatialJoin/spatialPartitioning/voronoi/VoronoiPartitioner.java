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

package cn.edu.whu.spatialJoin.spatialPartitioning.voronoi;

import cn.edu.whu.spatialJoin.enums.PartitionerType;
import cn.edu.whu.spatialJoin.joinJudgement.DedupParams;
import cn.edu.whu.spatialJoin.spatialPartitioning.KDB.KDBTree;
import cn.edu.whu.spatialJoin.spatialPartitioning.SpatialPartitioner;
import cn.edu.whu.spatialJoin.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.*;

public class VoronoiPartitioner
        extends SpatialPartitioner {
    public VoronoiPartitioner(VoronoiPartitioning voronoiPartitioning) {
        super(PartitionerType.VORONOI, voronoiPartitioning.getGrids());
    }

    @Override
    public int numPartitions() {
        return grids.size();
    }

    @Override
    public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject)
            throws Exception {

        Objects.requireNonNull(spatialObject, "spatialObject");

        final Envelope envelope = spatialObject.getEnvelopeInternal();

        Set<Tuple2<Integer, T>> result = new HashSet();
        for (int i = 0; i < grids.size(); i++) {
            final Envelope grid = grids.get(i);
            if (grid.intersects(envelope)) {
                result.add(new Tuple2<>(i, spatialObject));
            }
        }
        return result.iterator();
    }

    @Nullable
    @Override
    public DedupParams getDedupParams() {
        return new DedupParams(grids);
    }
}
