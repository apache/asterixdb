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

import static org.apache.asterix.metadata.utils.PushdownUtil.getConstant;
import static org.apache.asterix.metadata.utils.PushdownUtil.isAnd;
import static org.apache.asterix.metadata.utils.PushdownUtil.isCompare;
import static org.apache.asterix.metadata.utils.PushdownUtil.isConstant;
import static org.apache.asterix.metadata.utils.PushdownUtil.isFilterPath;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.DefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.UseDescriptor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

abstract class AbstractFilterPushdownProcessor extends AbstractPushdownProcessor {
    private final Set<ILogicalOperator> visitedOperators;

    public AbstractFilterPushdownProcessor(PushdownContext pushdownContext, IOptimizationContext context) {
        super(pushdownContext, context);
        visitedOperators = new HashSet<>();
    }

    @Override
    public final void process() throws AlgebricksException {
        List<ScanDefineDescriptor> scanDefineDescriptors = pushdownContext.getRegisteredScans();
        for (ScanDefineDescriptor scanDefineDescriptor : scanDefineDescriptors) {
            if (skip(scanDefineDescriptor)) {
                continue;
            }
            prepareScan(scanDefineDescriptor);
            pushdownFilter(scanDefineDescriptor, scanDefineDescriptor);
        }
    }

    /**
     * Should skip pushing down a filter for the given data-scan
     *
     * @param scanDefineDescriptor data-scan descriptor
     * @return true to skip, false otherwise
     */
    protected abstract boolean skip(ScanDefineDescriptor scanDefineDescriptor) throws AlgebricksException;

    /**
     * Prepare data-scan for a pushdown
     *
     * @param scanDefineDescriptor data-scan descriptor
     */
    protected abstract void prepareScan(ScanDefineDescriptor scanDefineDescriptor);

    /**
     * Prepare to pushdown a SELECT expression in the use-descriptor
     *
     * @param useDescriptor contains the SELECT operator and its expression
     */
    protected abstract void preparePushdown(UseDescriptor useDescriptor) throws AlgebricksException;

    /**
     * Is an expression pushable
     *
     * @param expression the expression to push down
     * @return true if it is pushable, false otherwise
     */
    protected abstract boolean isPushable(AbstractFunctionCallExpression expression);

    /**
     * Handle a compare function
     *
     * @param expression compare expression
     * @return true if the pushdown should continue, false otherwise
     */
    protected abstract boolean handleCompare(AbstractFunctionCallExpression expression) throws AlgebricksException;

    /**
     * Handle a value access path expression
     *
     * @param expression path expression
     * @return true if the pushdown should continue, false otherwise
     */
    protected abstract boolean handlePath(AbstractFunctionCallExpression expression) throws AlgebricksException;

    /**
     * Put the filter expression to data-scan
     *
     * @param scanDefineDescriptor data-scan descriptor
     * @param inlinedExpr          inlined filter expression
     */
    protected abstract void putFilterInformation(ScanDefineDescriptor scanDefineDescriptor,
            ILogicalExpression inlinedExpr) throws AlgebricksException;

    private void pushdownFilter(DefineDescriptor defineDescriptor, ScanDefineDescriptor scanDefineDescriptor)
            throws AlgebricksException {
        List<UseDescriptor> useDescriptors = pushdownContext.getUseDescriptors(defineDescriptor);
        for (UseDescriptor useDescriptor : useDescriptors) {
            /*
             * Pushdown works only if the scope(use) and scope(scan) are the same, as we cannot pushdown when
             * scope(use) > scope(scan) (e.g., after join or group-by)
             */
            ILogicalOperator useOperator = useDescriptor.getOperator();
            if (useDescriptor.getScope() == scanDefineDescriptor.getScope()
                    && useOperator.getOperatorTag() == LogicalOperatorTag.SELECT && isPushdownAllowed(useOperator)) {
                inlineAndPushdownFilter(useDescriptor, scanDefineDescriptor);
            } else if (useOperator.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                inlineAndPushdownFilter(useDescriptor, scanDefineDescriptor);
            }
        }

        for (UseDescriptor useDescriptor : useDescriptors) {
            DefineDescriptor nextDefineDescriptor = pushdownContext.getDefineDescriptor(useDescriptor);
            if (nextDefineDescriptor != null) {
                pushdownFilter(nextDefineDescriptor, scanDefineDescriptor);
            }
        }
    }

    private boolean isPushdownAllowed(ILogicalOperator useOperator) {
        Boolean disallowed = (Boolean) useOperator.getAnnotations()
                .getOrDefault(OperatorAnnotations.DISALLOW_FILTER_PUSHDOWN_TO_SCAN, Boolean.FALSE);
        return disallowed == Boolean.FALSE;
    }

    private void inlineAndPushdownFilter(UseDescriptor useDescriptor, ScanDefineDescriptor scanDefineDescriptor)
            throws AlgebricksException {
        ILogicalOperator selectOp = useDescriptor.getOperator();
        if (visitedOperators.contains(selectOp)) {
            // Skip and follow through to find any other selects that can be pushed down
            return;
        }

        // Get a clone of the SELECT expression and inline it
        ILogicalExpression inlinedExpr = pushdownContext.cloneAndInlineExpression(useDescriptor, context);
        // Prepare for pushdown
        preparePushdown(useDescriptor);
        if (pushdownFilterExpression(inlinedExpr)) {
            putFilterInformation(scanDefineDescriptor, inlinedExpr);
        }

        // Do not push down a select twice.
        visitedOperators.add(selectOp);
    }

    protected final boolean pushdownFilterExpression(ILogicalExpression expression) throws AlgebricksException {
        boolean pushdown = false;
        if (isConstant(expression)) {
            IAObject constantValue = getConstant(expression);
            // Only non-derived types are allowed
            pushdown = !constantValue.getType().getTypeTag().isDerivedType();
        } else if (isAnd(expression)) {
            pushdown = handleAnd((AbstractFunctionCallExpression) expression);
        } else if (isCompare(expression)) {
            pushdown = handleCompare((AbstractFunctionCallExpression) expression);
        } else if (isFilterPath(expression)) {
            pushdown = handlePath((AbstractFunctionCallExpression) expression);
        } else if (expression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            // All functions including OR
            pushdown = handleFunction((AbstractFunctionCallExpression) expression);
        }
        // PK variable should have (pushdown = false) as we should not involve the PK (at least currently)
        return pushdown;
    }

    private boolean handleAnd(AbstractFunctionCallExpression expression) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = expression.getArguments();
        Iterator<Mutable<ILogicalExpression>> argIter = args.iterator();
        while (argIter.hasNext()) {
            ILogicalExpression arg = argIter.next().getValue();
            // Allow for partial pushdown of AND operands
            if (!pushdownFilterExpression(arg)) {
                // Remove the expression that cannot be pushed down
                argIter.remove();
            }
        }
        return !args.isEmpty();
    }

    private boolean handleFunction(AbstractFunctionCallExpression expression) throws AlgebricksException {
        if (!expression.getFunctionInfo().isFunctional() || !isPushable(expression)) {
            return false;
        }

        for (Mutable<ILogicalExpression> argRef : expression.getArguments()) {
            ILogicalExpression arg = argRef.getValue();
            // Either all arguments are pushable or none
            if (!pushdownFilterExpression(arg)) {
                return false;
            }
        }
        return true;
    }
}
