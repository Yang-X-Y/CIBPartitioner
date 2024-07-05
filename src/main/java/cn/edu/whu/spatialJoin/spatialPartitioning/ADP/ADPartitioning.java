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

package cn.edu.whu.spatialJoin.spatialPartitioning.ADP;

import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;

public class ADPartitioning
        implements Serializable {

    /**
     * The KDBTree.
     */

    private final ADPTree partitionTree;

    public ADPartitioning(GridHistogramADP gridHistogramADP, int cellNum, int partitionNum, Envelope envelope) {
        partitionTree =  new ADPTree(gridHistogramADP,cellNum,envelope);
        partitionTree.constructTree(partitionNum);
    }


    public ADPTree getPartitionTree() {
        return this.partitionTree;
    }
}
