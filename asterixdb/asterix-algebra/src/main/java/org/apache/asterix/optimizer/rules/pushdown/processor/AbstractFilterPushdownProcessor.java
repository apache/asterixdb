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
import static org.apache.asterix.metadata.utils.PushdownUtil.getTypeEnv;
import static org.apache.asterix.metadata.utils.PushdownUtil.isAnd;
import static org.apache.asterix.metadata.utils.PushdownUtil.isCompare;
import static org.apache.asterix.metadata.utils.PushdownUtil.isConstant;
import static org.apache.asterix.metadata.utils.PushdownUtil.isFilterPath;
import static org.apache.asterix.metadata.utils.PushdownUtil.isSupportedFilterAggregateFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.DefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.UseDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.visitor.FilterExpressionInlineVisitor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

abstract class AbstractFilterPushdownProcessor extends AbstractPushdownProcessor {
    private final Set<ILogicalOperator> visitedOperators;
    private final Map<ILogicalOperator, List<UseDescriptor>> subplanSelects;
    private final List<UseDescriptor> scanCandidateFilters;
    private final Set<LogicalVariable> subplanProducedVariables;
    private final Queue<ILogicalOperator> subplanOpsQueue;

    public AbstractFilterPushdownProcessor(PushdownContext pushdownContext, IOptimizationContext context) {
        super(pushdownContext, context);
        visitedOperators = new HashSet<>();
        subplanSelects = new HashMap<>();
        scanCandidateFilters = new ArrayList<>();
        subplanProducedVariables = new HashSet<>();
        subplanOpsQueue = new LinkedList<>();
    }

    @Override
    public final boolean process() throws AlgebricksException {
        List<ScanDefineDescriptor> scanDefineDescriptors = pushdownContext.getRegisteredScans();
        boolean changed = false;
        for (ScanDefineDescriptor scanDefineDescriptor : scanDefineDescriptors) {
            if (skip(scanDefineDescriptor)) {
                continue;
            }
            subplanSelects.clear();
            scanCandidateFilters.clear();
            prepareScan(scanDefineDescriptor);
            collectFiltersInformation(scanDefineDescriptor, scanDefineDescriptor);
            putPotentialSelects(scanDefineDescriptor);
            changed |= pushdownFilter(scanDefineDescriptor);
        }
        return changed;
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
     * @param useDescriptor  contains the operator and its expression
     * @param scanDescriptor contains the scan definition where to push the filter expression
     */
    protected abstract void preparePushdown(UseDescriptor useDescriptor, ScanDefineDescriptor scanDescriptor)
            throws AlgebricksException;

    /**
     * Is an expression NOT pushable
     *
     * @param expression the expression to push down
     * @return true if it is NOT pushable, false otherwise
     */
    protected abstract boolean isNotPushable(AbstractFunctionCallExpression expression);

    /**
     * Handle a compare function
     *
     * @param expression        compare expression
     * @param currentDescriptor
     * @return true if the pushdown should continue, false otherwise
     */
    protected abstract FilterBranch handleCompare(AbstractFunctionCallExpression expression, int depth,
            UseDescriptor currentDescriptor) throws AlgebricksException;

    /**
     * Handle a value access path expression
     *
     * @param expression path expression
     * @return true if the pushdown should continue, false otherwise
     */
    protected final FilterBranch handlePath(AbstractFunctionCallExpression expression) throws AlgebricksException {
        IExpectedSchemaNode node = getPathNode(expression);
        if (node == null) {
            return FilterBranch.NA;
        }
        return handlePath(expression, node);
    }

    /**
     * Handle a value access path expression
     *
     * @param expression path expression
     * @param node       expected schema node (never null)
     * @return true if the pushdown should continue, false otherwise
     */
    protected abstract FilterBranch handlePath(AbstractFunctionCallExpression expression, IExpectedSchemaNode node)
            throws AlgebricksException;

    protected abstract IExpectedSchemaNode getPathNode(AbstractFunctionCallExpression expression)
            throws AlgebricksException;

    /**
     * Put the filter expression to data-scan
     *
     * @param scanDefineDescriptor data-scan descriptor
     * @param inlinedExpr          inlined filter expression
     */
    protected abstract void putFilterInformation(ScanDefineDescriptor scanDefineDescriptor,
            ILogicalExpression inlinedExpr) throws AlgebricksException;

    /**
     * Collects all the selects that appear at the same scope of 'defineDescriptor' and that are not part of a subplan
     *
     * @param defineDescriptor to get its use descriptors
     * @param scanDescriptor   data-scan descriptor
     */
    private void collectFiltersInformation(DefineDescriptor defineDescriptor, ScanDefineDescriptor scanDescriptor) {
        List<UseDescriptor> useDescriptors = pushdownContext.getUseDescriptors(defineDescriptor);

        // First find candidates for filter pushdowns
        for (UseDescriptor useDescriptor : useDescriptors) {
            if (visitedOperators.contains(useDescriptor.getOperator())) {
                continue;
            }
            if (canPushSelect(useDescriptor, scanDescriptor)) {
                scanCandidateFilters.add(useDescriptor);
            } else if (useDescriptor.getOperator().getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
                scanCandidateFilters.add(useDescriptor);
            }
        }

        // Next, descend using the def-use chain to find other candidates
        for (UseDescriptor useDescriptor : useDescriptors) {
            DefineDescriptor nextDefineDescriptor = pushdownContext.getDefineDescriptor(useDescriptor);
            if (nextDefineDescriptor != null) {
                collectFiltersInformation(nextDefineDescriptor, scanDescriptor);
            }
            visitedOperators.add(useDescriptor.getOperator());
        }
    }

    /**
     * If {@link #subplanSelects} is not empty, the check if the subplan correspond to some sub-filter
     * that cannot be linked using the def-use chain
     * Example:
     * <p>
     * select ($$26)
     * ... subplan
     * ... ... aggregate [$$26] <- [non-empty-stream()]
     * ... ... select (SOME_CONDITION)
     * <p>
     * In this example, the def-use chain cannot "chain" the nested SELECT with the upper SELECT 'select ($$26)' as the
     * function 'non-empty-stream()' is argument-less and does not use any variable originated from the data-scan.
     * However, we can do the "linking" by checking the produced variables of the subplan and find all their associated
     * {@link DefineDescriptor}. In the example, that would be the variable $$26 which is defined as
     * aggregate [$$26] <- [non-empty-stream()]. This would establish the connection between 'select ($$26)' and the
     * nested 'select (SOME_CONDITION)'
     *
     * @param scanDescriptor data-scan descriptor
     */
    private void putPotentialSelects(ScanDefineDescriptor scanDescriptor) throws AlgebricksException {
        subplanOpsQueue.clear();
        subplanOpsQueue.addAll(subplanSelects.keySet());
        while (!subplanOpsQueue.isEmpty()) {
            ILogicalOperator subplan = subplanOpsQueue.poll();
            subplanProducedVariables.clear();
            VariableUtilities.getProducedVariables(subplan, subplanProducedVariables);
            for (LogicalVariable producedVar : subplanProducedVariables) {
                DefineDescriptor defineDescriptor = pushdownContext.getDefineDescriptor(producedVar);
                if (defineDescriptor != null && !visitedOperators.contains(defineDescriptor.getOperator())
                        && isSupportedFilterAggregateFunction(defineDescriptor.getExpression())) {
                    // A define descriptor that has not been visited and has a supported filter aggregate function
                    // check for any missed SELECT
                    collectFiltersInformation(defineDescriptor, scanDescriptor);
                }
            }
        }
    }

    protected boolean pushdownFilter(ScanDefineDescriptor scanDescriptor) throws AlgebricksException {
        boolean changed = false;
        for (UseDescriptor candidate : scanCandidateFilters) {
            changed |= inlineAndPushdownFilter(candidate, scanDescriptor);
        }

        return changed;
    }

    private boolean canPushSelect(UseDescriptor useDescriptor, ScanDefineDescriptor scanDescriptor) {
        ILogicalOperator useOperator = useDescriptor.getOperator();
        /*
         * Pushdown works only if the scope(use) and scope(scan) are the same, as we cannot pushdown when
         * scope(use) > scope(scan) (e.g., after join or group-by)
         */
        if (useDescriptor.getScope() != scanDescriptor.getScope()) {
            return false;
        }

        // only select or data-scan are allowed (scan can have pushed condition)
        if (useOperator.getOperatorTag() != LogicalOperatorTag.SELECT
                && useOperator.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }

        // Do not push selects in sub-plan now. They will be pushed later on
        boolean inSubplan = useDescriptor.inSubplan();
        if (inSubplan && useOperator.getOperatorTag() == LogicalOperatorTag.SELECT) {
            ILogicalOperator subplanOp = useDescriptor.getSubplanOperator();
            List<UseDescriptor> selects = subplanSelects.get(subplanOp);
            if (selects == null) {
                subplanOpsQueue.add(subplanOp);
                subplanSelects.computeIfAbsent(subplanOp, k -> new ArrayList<>()).add(useDescriptor);
            } else {
                selects.add(useDescriptor);
            }
        }

        // Finally, push down if not in subplan
        return !inSubplan;
    }

    private boolean inlineAndPushdownFilter(UseDescriptor useDescriptor, ScanDefineDescriptor scanDefineDescriptor)
            throws AlgebricksException {
        boolean changed = false;

        FilterExpressionInlineVisitor inliningVisitor = pushdownContext.getInlineVisitor();
        // Get a clone of the operator's expression and inline it
        ILogicalExpression inlinedExpr = inliningVisitor.cloneAndInline(useDescriptor, subplanSelects);

        // Prepare for pushdown
        preparePushdown(useDescriptor, scanDefineDescriptor);
        if (pushdownFilterExpression(inlinedExpr, useDescriptor, 0) != FilterBranch.NA) {
            putFilterInformation(scanDefineDescriptor, inlinedExpr);
            changed = true;
        }

        return changed;
    }

    public enum FilterBranch {
        CONSTANT,
        AND,
        COMPARE,
        FILTER_PATH,
        FUNCTION,
        NA;

        public static FilterBranch andOutput(FilterBranch leftBranch, FilterBranch rightBranch,
                FilterBranch parentBranch) {
            if (leftBranch == FilterBranch.NA || rightBranch == FilterBranch.NA) {
                return FilterBranch.NA;
            }
            return parentBranch;
        }
    };

    protected final FilterBranch pushdownFilterExpression(ILogicalExpression expression, UseDescriptor useDescriptor,
            int depth) throws AlgebricksException {
        if (isConstant(expression)) {
            IAObject constantValue = getConstant(expression);
            // Only non-derived types are allowed
            if (!constantValue.getType().getTypeTag().isDerivedType()) {
                return FilterBranch.CONSTANT;
            }
            return FilterBranch.NA;
        } else if (isAnd(expression)) {
            return handleAnd((AbstractFunctionCallExpression) expression, depth, useDescriptor);
        } else if (isCompare(expression)) {
            return handleCompare((AbstractFunctionCallExpression) expression, depth, useDescriptor);
        } else if (isFilterPath(expression)) {
            return handlePath((AbstractFunctionCallExpression) expression);
        } else if (expression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            // All functions including OR
            return handleFunction((AbstractFunctionCallExpression) expression, depth, useDescriptor);
        }
        // PK variable should have (pushdown = false) as we should not involve the PK (at least currently)
        return FilterBranch.NA;
    }

    private FilterBranch handleAnd(AbstractFunctionCallExpression expression, int depth, UseDescriptor useDescriptor)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = expression.getArguments();
        Iterator<Mutable<ILogicalExpression>> argIter = args.iterator();
        while (argIter.hasNext()) {
            ILogicalExpression arg = argIter.next().getValue();
            // Allow for partial pushdown of AND operands
            if (pushdownFilterExpression(arg, useDescriptor, depth + 1) == FilterBranch.NA) {
                if (depth == 0) {
                    // Remove the expression that cannot be pushed down
                    argIter.remove();
                } else {
                    return FilterBranch.NA;
                }
            }
        }
        return !args.isEmpty() ? FilterBranch.AND : FilterBranch.NA;
    }

    protected boolean expressionReturnsArray(ILogicalExpression expression, ILogicalOperator operator)
            throws AlgebricksException {
        IAType expressionType = (IAType) context.getExpressionTypeComputer().getType(expression,
                context.getMetadataProvider(), getTypeEnv(operator, context));
        if (ATypeTag.UNION == expressionType.getTypeTag()) {
            expressionType = ((AUnionType) expressionType).getActualType();
        }
        return ATypeTag.ARRAY == expressionType.getTypeTag() || ATypeTag.ANY == expressionType.getTypeTag();
    }

    private FilterBranch handleFunction(AbstractFunctionCallExpression expression, int depth,
            UseDescriptor useDescriptor) throws AlgebricksException {
        if (!expression.getFunctionInfo().isFunctional() || isNotPushable(expression)) {
            return FilterBranch.NA;
        }

        for (Mutable<ILogicalExpression> argRef : expression.getArguments()) {
            ILogicalExpression arg = argRef.getValue();
            // Either all arguments are pushable or none
            if (pushdownFilterExpression(arg, useDescriptor, depth + 1) == FilterBranch.NA) {
                return FilterBranch.NA;
            }
        }
        return FilterBranch.FUNCTION;
    }
}
