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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;

/**
 * Needed only bc. current Hyrax operators require keys to be fields.
 */
public class ExtractGbyExpressionsRule extends AbstractExtractExprRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }

        if (context.checkIfInDontApplySet(this, op1)) {
            return false;
        }
        context.addToDontApplySet(this, op1);
        GroupByOperator g = (GroupByOperator) op1;
        boolean r1 = extractComplexExpressions(g, g.getGroupByList(), context);
        boolean r2 = extractComplexExpressions(g, g.getDecorList(), context);
        boolean fired = r1 || r2;
        if (fired) {
            context.computeAndSetTypeEnvironmentForOperator(g);
        }
        return fired;
    }

    private static boolean extractComplexExpressions(ILogicalOperator op,
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> exprList, IOptimizationContext context)
            throws AlgebricksException {
        return extractComplexExpressions(op, exprList, Pair::getSecond, context);
    }

    public static <T> boolean extractComplexExpressions(ILogicalOperator op, List<T> exprList,
            Function<T, Mutable<ILogicalExpression>> exprGetter, IOptimizationContext context)
            throws AlgebricksException {
        return extractComplexExpressions(op, exprList, exprGetter, t -> false, context);
    }
}
