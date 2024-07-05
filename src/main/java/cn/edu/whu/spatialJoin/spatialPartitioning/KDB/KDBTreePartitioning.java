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

package cn.edu.whu.spatialJoin.spatialPartitioning.KDB;

import org.locationtech.jts.geom.Envelope;
import java.io.Serializable;
import java.util.List;

public class KDBTreePartitioning
        implements Serializable {

    /**
     * The KDBTree.
     */

    private final KDBTree partitionTree;

    /**
     * Instantiates a new KDBTree partitioning.
     *
     * @param samples    the sample list
     * @param boundary   the boundary
     * @param partitionNum the partitionNum
     */

    public KDBTreePartitioning(List<Envelope> samples, Envelope boundary, int partitionNum)
            throws Exception {

        partitionTree = new KDBTree(samples.size() / partitionNum, partitionNum, boundary);
        for (final Envelope sample: samples) {
            partitionTree.insert(sample);
        }
        partitionTree.assignLeafIds();
    }

    public KDBTree getPartitionTree() {
        return this.partitionTree;
    }
}
