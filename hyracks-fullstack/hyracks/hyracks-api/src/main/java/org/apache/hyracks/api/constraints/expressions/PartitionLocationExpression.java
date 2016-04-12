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
package org.apache.hyracks.api.constraints.expressions;

import java.util.Collection;

import org.apache.hyracks.api.dataflow.OperatorDescriptorId;

public final class PartitionLocationExpression extends LValueConstraintExpression {
    private static final long serialVersionUID = 1L;

    private final OperatorDescriptorId opId;
    private final int partition;

    public PartitionLocationExpression(OperatorDescriptorId opId, int partition) {
        this.opId = opId;
        this.partition = partition;
    }

    @Override
    public ExpressionTag getTag() {
        return ExpressionTag.PARTITION_LOCATION;
    }

    public OperatorDescriptorId getOperatorDescriptorId() {
        return opId;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public void getChildren(Collection<ConstraintExpression> children) {
    }

    @Override
    protected void toString(StringBuilder buffer) {
        buffer.append(getTag()).append('(').append(opId.toString()).append(", ").append(partition).append(')');
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((opId == null) ? 0 : opId.hashCode());
        result = prime * result + partition;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PartitionLocationExpression other = (PartitionLocationExpression) obj;
        if (opId == null) {
            if (other.opId != null)
                return false;
        } else if (!opId.equals(other.opId))
            return false;
        if (partition != other.partition)
            return false;
        return true;
    }
}
