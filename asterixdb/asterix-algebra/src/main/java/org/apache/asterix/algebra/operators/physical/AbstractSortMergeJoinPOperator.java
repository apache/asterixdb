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
package org.apache.asterix.algebra.operators.physical;

import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;

public abstract class AbstractSortMergeJoinPOperator extends AbstractJoinPOperator {
    // Current for Single column Band, Theta join, will be extended onto multiple columns as well as Metric and Skyline join.

    protected List<LogicalVariable> keysLeftTopPartition;
    protected List<LogicalVariable> keysRightTopPartition;
    protected Pair<ILogicalExpression, ILogicalExpression> limitRange;
    protected ILogicalExpression partitionGranularity;

    public AbstractSortMergeJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight,
            Pair<ILogicalExpression, ILogicalExpression> range, ILogicalExpression gran) {
        super(kind, partitioningType);
        this.keysLeftTopPartition = sideLeft;
        this.keysRightTopPartition = sideRight;
        this.limitRange = range;
        this.partitionGranularity = gran;
    }

    public List<LogicalVariable> getKeysLeftTopPartition() {
        return keysLeftTopPartition;
    }

    public List<LogicalVariable> getKeysRightTopPartition() {
        return keysRightTopPartition;
    }

    public Pair<ILogicalExpression, ILogicalExpression> getLimitRange() {
        return limitRange;
    }

    public ILogicalExpression getGranularity() {
        return partitionGranularity;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        // TODO Auto-generated method stub

    }
}
