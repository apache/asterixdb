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
import java.util.logging.Logger;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;

public class BandSortMergeJoinPOperator extends AbstractSortMergeJoinPOperator {
    private final int memSizeInFrames;
    private final int maxInputBuildSizeInFrames;
    private final int slidingWindowSizeInFrames;
    private final int aveRecordsPerFrame;
    private final double fudgeFactor;

    private static final Logger LOGGER = Logger.getLogger(BandSortMergeJoinPOperator.class.getName());

    public BandSortMergeJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight,
            Pair<ILogicalExpression, ILogicalExpression> range, ILogicalExpression gran, int memSizeInFrames,
            int maxInputBuildSizeInFrames, int slidingWindowSizeInFrames, int aveRecordsPerFrame, double fudgeFactor) {
        super(kind, partitioningType, sideLeft, sideRight, range, gran);
        this.memSizeInFrames = memSizeInFrames;
        this.maxInputBuildSizeInFrames = maxInputBuildSizeInFrames;
        this.slidingWindowSizeInFrames = slidingWindowSizeInFrames;
        this.aveRecordsPerFrame = aveRecordsPerFrame;
        this.fudgeFactor = fudgeFactor;

        LOGGER.fine("BandSortMergeJoinPOperator constructed with: JoinKind: " + kind + " JoinPartitioningType="
                + partitioningType + " List<LogicalVariable>=" + sideLeft + " List<LogicalVariable=" + sideRight
                + " Pair<ILogicalExpression,ILogicalExpression>=" + range + " ILogicalExpression=" + gran
                + " memSizeInFrames=" + memSizeInFrames + " maxInputBuildSizeInFrames=" + maxInputBuildSizeInFrames
                + " slidingWindowSizeInFrames=" + slidingWindowSizeInFrames + " aveRecordsPerFrame="
                + aveRecordsPerFrame + " fudgeFactor=" + fudgeFactor);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        // TODO Auto-generated method stub
        return PhysicalOperatorTag.BAND_SORTMERGE_JOIN;
    }

    @Override
    public boolean isMicroOperator() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        // TODO Auto-generated method stub

    }
}
