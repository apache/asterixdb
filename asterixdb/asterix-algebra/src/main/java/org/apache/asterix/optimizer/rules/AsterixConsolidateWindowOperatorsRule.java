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

package org.apache.asterix.optimizer.rules;

import java.util.List;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.rewriter.rules.ConsolidateWindowOperatorsRule;

/**
 * Asterix-specific rule that enables consolidation of window operators in the following additional cases
 * <ul>
 * <li> Different {@link WindowOperator#getFrameMaxObjects()} values if aggregate function is
 *      {@link BuiltinFunctions#FIRST_ELEMENT}
 * </li>
 * </ul>
 */
public final class AsterixConsolidateWindowOperatorsRule extends ConsolidateWindowOperatorsRule {

    @Override
    protected boolean subsumeFrameMaxObjects(int maxObjects1, int maxObjects2, AggregateOperator aggOp2) {
        if (allFunctionCalls(aggOp2.getExpressions(), BuiltinFunctions.FIRST_ELEMENT) && maxObjects2 >= 1
                && (maxObjects1 == WindowOperator.FRAME_MAX_OBJECTS_UNLIMITED || maxObjects1 >= maxObjects2)) {
            return true;
        }
        return super.subsumeFrameMaxObjects(maxObjects1, maxObjects2, aggOp2);
    }

    private boolean allFunctionCalls(List<Mutable<ILogicalExpression>> exprRefs, FunctionIdentifier fid) {
        for (Mutable<ILogicalExpression> exprRef : exprRefs) {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return false;
            }
            AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
            if (!callExpr.getFunctionIdentifier().equals(fid)) {
                return false;
            }
        }
        return true;
    }
}