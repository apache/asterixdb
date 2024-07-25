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
package org.apache.asterix.optimizer.rules.pushdown.processor;

import static org.apache.asterix.metadata.utils.PushdownUtil.isAnd;
import static org.apache.asterix.metadata.utils.PushdownUtil.isOr;
import static org.apache.asterix.metadata.utils.PushdownUtil.isSameFunction;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.optimizer.rules.InlineAndRemoveRedundantBooleanExpressionsRule;
import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

/**
 * Inline and normalize filter expressions of a scan.
 * Inline example:
 * and(a > 2) --> a > 2
 * and(a > 2, and(b > 2)) --> and(a > 2, b > 2)
 * Normalization example
 * and(a, a) -> a
 */
public class InlineAndNormalizeFilterExpressionsProcessor extends AbstractPushdownProcessor {
    public InlineAndNormalizeFilterExpressionsProcessor(PushdownContext pushdownContext, IOptimizationContext context) {
        super(pushdownContext, context);
    }

    @Override
    public boolean process() throws AlgebricksException {
        List<ScanDefineDescriptor> scanDefineDescriptors = pushdownContext.getRegisteredScans();
        for (ScanDefineDescriptor scanDefineDescriptor : scanDefineDescriptors) {
            scanDefineDescriptor.setFilterExpression(inline(scanDefineDescriptor.getFilterExpression()));
            scanDefineDescriptor.setRangeFilterExpression(inline(scanDefineDescriptor.getRangeFilterExpression()));
        }
        // Should always return false as this processor relies on filter pushdown
        return false;
    }

    private ILogicalExpression inline(ILogicalExpression expression) {
        if (expression == null || expression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return expression;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        if ((isAnd(funcExpr) || isOr(funcExpr)) && args.size() == 1) {
            // Fix the incorrect AND with a single argument. This could happen if the AND expression is partially pushed
            return inline(args.get(0).getValue());
        }

        if (isAnd(funcExpr) || isOr(funcExpr)) {
            List<Mutable<ILogicalExpression>> inlinedArgs = new ArrayList<>();
            for (Mutable<ILogicalExpression> argRef : args) {
                // inline arg first
                ILogicalExpression arg = inline(argRef.getValue());

                if (isSameFunction(funcExpr, arg)) {
                    // funcExpr is AND/OR and arg is also the same. Inline AND/OR by adding arg arguments to funcExpr
                    AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) arg;
                    inlinedArgs.addAll(argFuncExpr.getArguments());
                } else {
                    // funcExpr and arg are different. Set the inlined arg to argRef (argRef has un-inlined version).
                    argRef.setValue(arg);
                    inlinedArgs.add(argRef);
                }
            }

            // Clear the original argument
            args.clear();
            // Add the new inlined arguments
            args.addAll(inlinedArgs);
        }

        // Remove redundant expressions from AND/OR
        if (isAnd(funcExpr) || isOr(funcExpr)) {
            InlineAndRemoveRedundantBooleanExpressionsRule.removeRedundantExpressions(args);
            if (args.size() == 1) {
                // InlineAndRemoveRedundantBooleanExpressionsRule produced a single argument return it
                return args.get(0).getValue();
            }
        }

        // either the original expression or the inlined AND/OR expression
        return expression;
    }
}
