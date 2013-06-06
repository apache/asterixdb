/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;

public abstract class AbstractJoinPOperator extends AbstractPhysicalOperator {

    public enum JoinPartitioningType {
        PAIRWISE, BROADCAST
    }

    protected final JoinKind kind;
    protected final JoinPartitioningType partitioningType;

    public AbstractJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType) {
        this.kind = kind;
        this.partitioningType = partitioningType;
    }

    public JoinKind getKind() {
        return kind;
    }

    public JoinPartitioningType getPartitioningType() {
        return partitioningType;
    }
}
