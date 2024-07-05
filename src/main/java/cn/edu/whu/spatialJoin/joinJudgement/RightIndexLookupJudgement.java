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
//public class RightIndexLookupJudgement<T extends Geometry, U extends Geometry>
//        extends JudgementBase
//        implements FlatMapFunction2<Iterator<T>, Iterator<SpatialIndex>, Pair<T, U>>, Serializable {
//
//    final static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(RightIndexLookupJudgement.class);
//
//    HashMap<String, Geometry> lookupTable;
//    /**
//     * @see JudgementBase
//     */
//    public RightIndexLookupJudgement(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams) {
//        super(considerBoundaryIntersection, dedupParams);
//    }
//
//    @Override
//    public Iterator<Pair<T, U>> call(Iterator<T> streamShapes, Iterator<SpatialIndex> indexIterator)
//            throws Exception {
//        List<Pair<T, U>> result = new ArrayList<>();
//
//        if (!indexIterator.hasNext() || !streamShapes.hasNext()) {
//            return result.iterator();
//        }
//
//        initPartition();
//        int count = 0;
//        int caSum = 0;
//        int trueHitNum = 0;
//        int reNum = 0;
//        double refineCost = 0.0;
//        SpatialIndex treeIndex = indexIterator.next();
//        if (treeIndex instanceof GridPrefixTree) {
//            lookupTable = ((GridPrefixTree) treeIndex).getLookupTable();
////            System.out.println("lookupTable size:"+lookupTable.size());
//        }
//        while (streamShapes.hasNext()) {
//            T streamShape = streamShapes.next();
//            List<Geometry> candidates;
//            count++;
//            //if(count < 5){
//            if (treeIndex instanceof AdaptiveQuadTreeIndex) {
//                /*if(streamShape instanceof GridPolygon){
//                    candidates = treeIndex.query(new AdaptiveGrid(((GridPolygon)streamShape).getGrids()));
//                }else if (streamShape instanceof GridLineString){
//                    candidates = treeIndex.query(new AdaptiveGrid(((GridLineString)streamShape).getGrids()));
//                }else{
//                    candidates = treeIndex.query(new AdaptiveGrid(((GridPoint)streamShape).getGrid()));
//                }*/
//                candidates = treeIndex.query(AdaptiveGrid.transformGridGeometry(streamShape));
//            } else if (treeIndex instanceof GridPrefixTreeIndex) {
//                HashMap<String, List<Geometry>> gptQueryResult = ((GridPrefixTreeIndex) treeIndex).query((GridGeometry) streamShape);
//                List<Geometry> trueHits = gptQueryResult.get("trueHit");
//                candidates = gptQueryResult.get("candidate");
//                for (Geometry trueHit : trueHits) {
//                    reNum++;
//                    result.add(Pair.of(streamShape, (U) trueHit));
//                }
//            } else if (treeIndex instanceof GridPrefixTree) {
//                GPTPVisitor gptpVisitor = ((GridPrefixTree) treeIndex).query((GridGeometry) streamShape);
////                if (gptpVisitor.getCandidateItems().size()!=0) {
////                    System.out.println("getCandidateItems:"+gptpVisitor.getCandidateItems().size());
////                    System.out.println("getTrueHitItems:"+gptpVisitor.getTrueHitItems().size());
////                }
//
//                List<Geometry> tempCandidates = new ArrayList<>();
//                for(String objectId : gptpVisitor.getCandidateItems()){
////                    System.out.println("lookuptable size:"+lookupTable.size());
//                    tempCandidates.add(lookupTable.get(objectId));
////                    System.out.println("objectId:"+objectId);
//                }
//                for (String trueHit : gptpVisitor.getTrueHitItems()) {
////                    System.out.println("objectId:"+trueHit);
////                    tempCandidates.add(lookupTable.get(trueHit));
//                    Geometry right = lookupTable.get(trueHit);
//                    if (streamShape.isValid() && right.isValid()
//                            && (referencePointCheck(streamShape, right))) {
//                            trueHitNum++;
//                            result.add(Pair.of(streamShape, (U) right));
//                    }
//                }
//                candidates = tempCandidates;
//            } else {
//                candidates = treeIndex.query(streamShape.getEnvelopeInternal());
//            }
//            caSum += candidates.size();
//
//            for (Geometry candidate : candidates) {
////                String printRes = "GEOMETRYCOLLECTION(" + streamShape.toText() +
////                        "," + candidate.toText() +  "," + envelopePolygon.toText()+ ")";
////                System.out.println(printRes);
//                // Refine phase. Use the real polygon (instead of its MBR) to recheck the spatial relation.
//                refineCost += ((candidate.getNumPoints()+streamShape.getNumPoints())*Math.log(candidate.getNumPoints()+streamShape.getNumPoints()));
//                if (match(streamShape, candidate)) {
//                    reNum++;
//                    result.add(Pair.of(streamShape, (U) candidate));
//                }
//            }
//        }//}
////        System.out.println("partitionId " + TaskContext.getPartitionId() + " refineCost:"+refineCost);
//
////        log.info("partitionId " + TaskContext.getPartitionId() + " partition envelope: " + getDedupParams().getPartitionExtents().get(TaskContext.getPartitionId()));
////        log.info("partitionId " + TaskContext.getPartitionId() + " search geometry Num: " + count);
////        log.info("partitionId " + TaskContext.getPartitionId() + " get candidates Num: " + caSum);
////        log.info("partitionId " + TaskContext.getPartitionId() + " final result after filter: " + reNum);
////        System.out.println("partitionId " + TaskContext.getPartitionId() + " searchNum:"+count+ " candidatesNum: " + caSum+" trueHitNum:"+trueHitNum+" resultNum:"+reNum);
//        return result.iterator();
//    }
//}
