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

import org.apache.asterix.algebra.operators.physical.IntervalMergeJoinPOperator;
import org.apache.asterix.algebra.operators.physical.SpatialJoinPOperator;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtilFactory;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;

import java.util.Collection;
import java.util.List;

public class SpatialJoinUtils {

    protected static FunctionIdentifier isSpatialJoinCondition(ILogicalExpression e,
                                                               Collection<LogicalVariable> inLeftAll, Collection<LogicalVariable> inRightAll,
                                                               Collection<LogicalVariable> outLeftFields, Collection<LogicalVariable> outRightFields, int left,
                                                               int right) {
        FunctionIdentifier fiReturn;
        boolean switchArguments = false;

        if (e.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) e;
        FunctionIdentifier fi = funcExpr.getFunctionIdentifier();
        if (BuiltinFunctions.isSpatialFilterFunction(fi)) {
            fiReturn = fi;
        } else {
            return null;
        }

        ILogicalExpression opLeft = funcExpr.getArguments().get(left).getValue();
        ILogicalExpression opRight = funcExpr.getArguments().get(right).getValue();

        if (opLeft.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || opRight.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return null;
        }

        LogicalVariable var1 = ((VariableReferenceExpression) opLeft).getVariableReference();
        if (inLeftAll.contains(var1) && !outLeftFields.contains(var1)) {
            outLeftFields.add(var1);
        } else if (inRightAll.contains(var1) && !outRightFields.contains(var1)) {
            outRightFields.add(var1);
        } else {
            return null;
        }

        LogicalVariable var2 = ((VariableReferenceExpression) opRight).getVariableReference();
        if (inLeftAll.contains(var2) && !outLeftFields.contains(var2) && switchArguments) {
            outLeftFields.add(var2);
        } else if (inRightAll.contains(var2) && !outRightFields.contains(var2) && !switchArguments) {
            outRightFields.add(var2);
        } else {
            return null;
        }

        return fiReturn;
    }

    protected static void setSpatialJoinOp(AbstractBinaryJoinOperator op, FunctionIdentifier fi, List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, IOptimizationContext context) throws CompilationException {
        op.setPhysicalOperator(new SpatialJoinPOperator(op.getJoinKind(), AbstractJoinPOperator.JoinPartitioningType.BROADCAST, sideLeft, sideRight));
    }
}
