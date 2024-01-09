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

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * Inline and remove redundant boolean expressions
 * <p>
 * Inline Example:
 * and(x, and(y, and(z, w))) -> and(x, y, z, w)
 * <p>
 * Remove redundant example:
 * or(x, y, y) -> or(x, y)
 * TODO(wyk) include this rule in {@link org.apache.asterix.optimizer.base.RuleCollections}
 */
public class InlineAndRemoveRedundantBooleanExpressionsRule extends AbstractConditionExpressionRule {

    @Override
    protected boolean transform(Mutable<ILogicalExpression> condRef) {
        AbstractFunctionCallExpression function = getFunctionExpression(condRef.getValue());
        if (function == null) {
            return false;
        }

        boolean changed = false;
        for (Mutable<ILogicalExpression> argRef : function.getArguments()) {
            changed |= transform(argRef);
        }

        final FunctionIdentifier fid = function.getFunctionIdentifier();
        if (AlgebricksBuiltinFunctions.AND.equals(fid) || AlgebricksBuiltinFunctions.OR.equals(fid)) {
            changed |= inlineCondition(function);
            changed |= removeRedundantExpressions(function.getArguments());

            //Special case: disjuncts/conjuncts have been factored out into a single (non-disjunct/conjunct) expression
            if (function.getArguments().size() == 1) {
                final ILogicalExpression newCond = function.getArguments().get(0).getValue();
                condRef.setValue(newCond);
            }
        }

        return changed;
    }

    private boolean inlineCondition(AbstractFunctionCallExpression function) {
        final FunctionIdentifier fid = function.getFunctionIdentifier();
        final List<Mutable<ILogicalExpression>> args = function.getArguments();

        int i = 0;
        boolean changed = false;
        while (i < args.size()) {
            final AbstractFunctionCallExpression argFunction = getFunctionExpression(args.get(i).getValue());
            if (argFunction != null && fid.equals(argFunction.getFunctionIdentifier())) {
                args.remove(i);
                args.addAll(i, argFunction.getArguments());
                changed = true;
            } else {
                i++;
            }
        }

        return changed;
    }

    public static boolean removeRedundantExpressions(List<Mutable<ILogicalExpression>> exprs) {
        final int originalSize = exprs.size();
        int i = 0;
        while (i < exprs.size()) {
            int j = i + 1;
            while (j < exprs.size()) {
                if (FunctionUtil.commutativeEquals(exprs.get(i).getValue(), exprs.get(j).getValue())) {
                    exprs.remove(j);
                } else {
                    j++;
                }
            }
            i++;
        }

        return exprs.size() != originalSize;
    }

}
