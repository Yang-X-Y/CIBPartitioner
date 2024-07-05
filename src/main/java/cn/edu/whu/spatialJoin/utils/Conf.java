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

package cn.edu.whu.spatialJoin.utils;

import cn.edu.whu.spatialJoin.enums.PartitionerType;
import cn.edu.whu.spatialJoin.enums.IndexType;
import cn.edu.whu.spatialJoin.enums.JoinBuildSide;
import cn.edu.whu.spatialJoin.enums.JoinSparitionDominantSide;
import org.apache.spark.SparkConf;
import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.lang.reflect.Field;

public class Conf
        implements Serializable {

    // Global parameters of SQL. All these parameters can be initialized through SparkConf.

    private Boolean useIndex = false;

    private IndexType indexType = IndexType.QUADTREE;

    // Parameters for JoinQuery including RangeJoin and DistanceJoin

    private JoinSparitionDominantSide joinSparitionDominantSide = JoinSparitionDominantSide.LEFT;

    private JoinBuildSide joinBuildSide = JoinBuildSide.LEFT;

    private Long joinApproximateTotalCount = (long) -1;

    private Envelope datasetBoundary = new Envelope(0, 0, 0, 0);

    private Integer fallbackPartitionNum = -1;

    private PartitionerType joinPartitionerType = PartitionerType.KDBTREE;

    public Conf(SparkConf sparkConf) {
        this.useIndex = sparkConf.getBoolean("sql.global.index", true);
        this.indexType = IndexType.getIndexType(sparkConf.get("sql.global.indextype", "quadtree"));
        this.joinApproximateTotalCount = sparkConf.getLong("sql.join.approxcount", -1);
        String[] boundaryString = sparkConf.get("sql.join.boundary", "0,0,0,0").split(",");
        this.datasetBoundary = new Envelope(Double.parseDouble(boundaryString[0]), Double.parseDouble(boundaryString[0]),
                Double.parseDouble(boundaryString[0]), Double.parseDouble(boundaryString[0]));
        this.joinPartitionerType = PartitionerType.getGridType(sparkConf.get("sql.join.gridtype", "kdbtree"));
        this.joinBuildSide = JoinBuildSide.getBuildSide(sparkConf.get("sql.join.indexbuildside", "right"));
        this.joinSparitionDominantSide = JoinSparitionDominantSide.getJoinSparitionDominantSide(sparkConf.get("sql.join.spatitionside", "right"));
        this.fallbackPartitionNum = sparkConf.getInt("sql.join.numpartition", -1);
    }

    public Boolean getUseIndex() {
        return useIndex;
    }

    public void setUseIndex(Boolean useIndex) {
        this.useIndex = useIndex;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public Long getJoinApproximateTotalCount() {
        return joinApproximateTotalCount;
    }

    public void setJoinApproximateTotalCount(Long joinApproximateTotalCount) {
        this.joinApproximateTotalCount = joinApproximateTotalCount;
    }

    public Envelope getDatasetBoundary() {
        return datasetBoundary;
    }

    public void setDatasetBoundary(Envelope datasetBoundary) {
        this.datasetBoundary = datasetBoundary;
    }

    public JoinBuildSide getJoinBuildSide() {
        return joinBuildSide;
    }

    public void setJoinBuildSide(JoinBuildSide joinBuildSide) {
        this.joinBuildSide = joinBuildSide;
    }

    public PartitionerType getJoinGridType() {
        return joinPartitionerType;
    }

    public void setJoinGridType(PartitionerType joinPartitionerType) {
        this.joinPartitionerType = joinPartitionerType;
    }

    public JoinSparitionDominantSide getJoinSparitionDominantSide() {
        return joinSparitionDominantSide;
    }

    public void setJoinSparitionDominantSide(JoinSparitionDominantSide joinSparitionDominantSide) {
        this.joinSparitionDominantSide = joinSparitionDominantSide;
    }

    public Integer getFallbackPartitionNum() {
        return fallbackPartitionNum;
    }

    public void setFallbackPartitionNum(Integer fallbackPartitionNum) {
        this.fallbackPartitionNum = fallbackPartitionNum;
    }

    public String toString() {
        try {
            String sb = "";
            Class<?> objClass = this.getClass();
            sb += "SQL Configuration:\n";
            Field[] fields = objClass.getDeclaredFields();
            for (Field field : fields) {
                String name = field.getName();
                Object value = field.get(this);
                sb += name + ": " + value.toString() + "\n";
            }
            return sb;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
