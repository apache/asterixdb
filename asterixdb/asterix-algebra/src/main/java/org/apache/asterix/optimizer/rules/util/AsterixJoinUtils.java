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
package org.apache.asterix.optimizer.rules.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.annotations.RangeAnnotation;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

public class AsterixJoinUtils {

    private static final int LEFT = 0;
    private static final int RIGHT = 1;

    private AsterixJoinUtils() {
    }

    public static void setJoinAlgorithmAndExchangeAlgo(AbstractBinaryJoinOperator op, Boolean topLevelOp,
            IOptimizationContext context) throws AlgebricksException {
        if (!topLevelOp) {
            return;
        }
        ILogicalExpression conditionLE = op.getCondition().getValue();
        if (conditionLE.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }
        List<LogicalVariable> sideLeft = new ArrayList<>(1);
        List<LogicalVariable> sideRight = new ArrayList<>(1);
        List<LogicalVariable> varsLeft = op.getInputs().get(LEFT).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(RIGHT).getValue().getSchema();
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) conditionLE;
        FunctionIdentifier fi =
                IntervalJoinUtils.isIntervalJoinCondition(fexp, varsLeft, varsRight, sideLeft, sideRight, LEFT, RIGHT);
        if (fi == null) {
            return;
        }
        RangeAnnotation rangeAnnotation = IntervalJoinUtils.findRangeAnnotation(fexp);
        if (rangeAnnotation == null) {
            return;
        }
        //Check RangeMap type
        RangeMap rangeMap = rangeAnnotation.getRangeMap();
        if (rangeMap.getTag(0, 0) != ATypeTag.DATETIME.serialize() && rangeMap.getTag(0, 0) != ATypeTag.DATE.serialize()
                && rangeMap.getTag(0, 0) != ATypeTag.TIME.serialize()) {
            IWarningCollector warningCollector = context.getWarningCollector();
            if (warningCollector.shouldWarn()) {
                warningCollector.warn(Warning.forHyracks(op.getSourceLocation(), ErrorCode.INAPPLICABLE_HINT,
                        "Date, DateTime, and Time are only range hints types supported for interval joins"));
            }
            return;
        }
        IntervalPartitions intervalPartitions =
                IntervalJoinUtils.createIntervalPartitions(op, fi, sideLeft, sideRight, rangeMap, context, LEFT, RIGHT);
        IntervalJoinUtils.setSortMergeIntervalJoinOp(op, fi, sideLeft, sideRight, context, intervalPartitions);
    }
}
