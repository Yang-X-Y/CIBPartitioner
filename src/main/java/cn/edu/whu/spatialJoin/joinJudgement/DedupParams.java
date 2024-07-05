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

import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Contains information necessary to activate de-dup logic in sub-classes of {@link JudgementBase}.
 */
public final class DedupParams
        implements Serializable {
    private final List<Envelope> partitionExtents;
    private List<Double> estimatedCosts;

    /**
     * @param partitionExtents A list of partition extents in such an order that
     *                         an index of an element in this list matches partition ID.
     */
    public DedupParams(List<Envelope> partitionExtents) {
        this.partitionExtents = Objects.requireNonNull(partitionExtents, "partitionExtents");
    }

    public DedupParams(List<Envelope> partitionExtents,List<Double> estimatedCosts) {
        this.partitionExtents = Objects.requireNonNull(partitionExtents, "partitionExtents");
        this.estimatedCosts = estimatedCosts;
    }

    public List<Envelope> getPartitionExtents() {
        return partitionExtents;
    }

    public List<Double> getEstimatedCosts() {
        return estimatedCosts;
    }
}
