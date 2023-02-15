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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public abstract class AbstractConditionExpressionRule implements IAlgebraicRewriteRule {
    private IOptimizationContext context;

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        final ILogicalOperator op = opRef.getValue();
        final Mutable<ILogicalExpression> condRef;
        switch (op.getOperatorTag()) {
            case SELECT:
                final SelectOperator select = (SelectOperator) op;
                condRef = select.getCondition();
                break;
            case INNERJOIN:
            case LEFTOUTERJOIN:
                final AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) op;
                condRef = join.getCondition();
                break;
            default:
                return false;
        }

        this.context = context;

        boolean changed = transform(condRef);
        if (changed) {
            context.computeAndSetTypeEnvironmentForOperator(op);
        }

        return changed;
    }

    protected final AbstractFunctionCallExpression getFunctionExpression(ILogicalExpression expression) {
        if (expression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }

        return (AbstractFunctionCallExpression) expression;
    }

    protected final IFunctionInfo getFunctionInfo(FunctionIdentifier fid) {
        return context.getMetadataProvider().lookupFunction(fid);
    }

    /**
     * Transform condition expression
     *
     * @param condRef SELECT or join condition reference
     * @return {@code <code>true</code>} condition has been modified
     * {@code <code>false</code>} otherwise.
     */
    protected abstract boolean transform(Mutable<ILogicalExpression> condRef);
}
