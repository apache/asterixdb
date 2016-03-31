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

public final class PartitionCountExpression extends LValueConstraintExpression {
    private static final long serialVersionUID = 1L;

    private final OperatorDescriptorId opId;

    public PartitionCountExpression(OperatorDescriptorId opId) {
        this.opId = opId;
    }

    @Override
    public ExpressionTag getTag() {
        return ExpressionTag.PARTITION_COUNT;
    }

    public OperatorDescriptorId getOperatorDescriptorId() {
        return opId;
    }

    @Override
    public void getChildren(Collection<ConstraintExpression> children) {
    }

    @Override
    protected void toString(StringBuilder buffer) {
        buffer.append(getTag()).append('(').append(opId.toString()).append(')');
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((opId == null) ? 0 : opId.hashCode());
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
        PartitionCountExpression other = (PartitionCountExpression) obj;
        if (opId == null) {
            if (other.opId != null)
                return false;
        } else if (!opId.equals(other.opId))
            return false;
        return true;
    }
}
