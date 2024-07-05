///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package cn.edu.whu.spatialJoin.joinJudgement;
//
//import cn.edu.whu.spatialJoin.JTS.GridGeometry;
//import cn.edu.whu.spatialJoin.spatialPartitioning.adaptiveGridQuad.AdaptiveGrid;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.spark.api.java.function.FlatMapFunction2;
//import org.locationtech.jts.geom.Geometry;
//import org.locationtech.jts.index.SpatialIndex;
//import org.locationtech.jts.index.adaptivequadtree.AdaptiveQuadTreeIndex;
//import org.locationtech.jts.index.gridPrefixTree.GridPrefixTreeIndex;
//import org.locationtech.jts.index.gridPrefixTreePrune.GPTPVisitor;
//import org.locationtech.jts.index.gridPrefixTreePrune.GridPrefixTree;
//
//import javax.annotation.Nullable;
//import java.io.Serializable;
//import java.util.*;
//
//public class LeftIndexLookupJudgement<T extends Geometry, U extends Geometry>
//        extends JudgementBase
//        implements FlatMapFunction2<Iterator<SpatialIndex>, Iterator<U>, Pair<T, U>>, Serializable {
//
//    /**
//     * @see JudgementBase
//     */
//    public LeftIndexLookupJudgement(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams) {
//        super(considerBoundaryIntersection, dedupParams);
//    }
//
//    @Override
//    public Iterator<Pair<T, U>> call(Iterator<SpatialIndex> indexIterator, Iterator<U> streamShapes)
//            throws Exception {
//        List<Pair<T, U>> result = new ArrayList<>();
//
//        if (!indexIterator.hasNext() || !streamShapes.hasNext()) {
//            return result.iterator();
//        }
//
//        initPartition();
//
//        SpatialIndex treeIndex = indexIterator.next();
//        while (streamShapes.hasNext()) {
//            U streamShape = streamShapes.next();
//            List<Geometry> candidates;
//            if (treeIndex instanceof AdaptiveQuadTreeIndex) {
//                candidates = treeIndex.query(AdaptiveGrid.transformGridGeometry(streamShape));
//                /*if(streamShape instanceof GridPolygon){
//                    candidates = treeIndex.query(new AdaptiveGrid(((GridPolygon)streamShape).getGrids()));
//                }else if (streamShape instanceof GridLineString){
//                    candidates = treeIndex.query(new AdaptiveGrid(((GridLineString)streamShape).getGrids()));
//                }else{
//                    candidates = treeIndex.query(new AdaptiveGrid(((GridPoint)streamShape).getGrid()));
//                }*/
//            } else if (treeIndex instanceof GridPrefixTreeIndex) {
//                HashMap<String, List<Geometry>> gptQueryResult = ((GridPrefixTreeIndex) treeIndex).query((GridGeometry) streamShape);
//                List<Geometry> trueHits = gptQueryResult.get("trueHit");
//                candidates = gptQueryResult.get("candidate");
//                for (Geometry trueHit : trueHits) {
//                    result.add(Pair.of((T) trueHit,streamShape));
//                }
//            } else if (treeIndex instanceof GridPrefixTree) {
//                GPTPVisitor gptpVisitor = ((GridPrefixTree) treeIndex).query((GridGeometry) streamShape);
//                HashMap<String, Geometry> lookupTable = ((GridPrefixTree) treeIndex).getLookupTable();
//                List<Geometry> tempCandidates = new ArrayList<>();
//                for(String objectId : gptpVisitor.getCandidateItems()){
//                    tempCandidates.add(lookupTable.get(objectId));
//                }
//                candidates = tempCandidates;
//                for (String trueHit : gptpVisitor.getTrueHitItems()) {
//                    Geometry trueHitObject = lookupTable.get(trueHit);
//                    if (referencePointCheck(trueHitObject,streamShape)) {
//                        result.add(Pair.of((T) trueHitObject,streamShape));
//                    }
//                }
//            } else {
//                //System.out.println("streamShape.getEnvelopeInternal():"+ Arrays.toString(streamShape.getCoordinates()));
//                candidates = treeIndex.query(streamShape.getEnvelopeInternal());
//            }
//            for (Geometry candidate : candidates) {
//                // Refine phase. Use the real polygon (instead of its MBR) to recheck the spatial relation.
//                if (match(candidate, streamShape)) {
//                    result.add(Pair.of((T) candidate, streamShape));
//                }
//            }
//        }
//        return result.iterator();
//    }
//}
