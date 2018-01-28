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
package org.apache.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.asterix.algebra.operators.CommitOperator;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * This rule optimizes simple selections with secondary or primary indexes. The use of an
 * index is expressed as an unnest-map over an index-search function which will be
 * replaced with the appropriate embodiment during codegen.
 * Matches the following operator patterns:
 * Standard secondary index pattern:
 * There must be at least one assign, but there may be more, e.g., when matching similarity-jaccard-check().
 * (select) <-- (assign | unnest)+ <-- (datasource scan)
 * Primary index lookup pattern:
 * Since no assign is necessary to get the primary key fields (they are already stored fields in the BTree tuples).
 * (select) <-- (datasource scan)
 * Replaces the above patterns with this plan:
 * (select) <-- (assign) <-- (btree search) <-- (sort) <-- (unnest-map(index search)) <-- (assign)
 * The sort is optional, and some access methods implementations may choose not to sort.
 * Note that for some index-based optimizations we do not remove the triggering
 * condition from the select, since the index may only acts as a filter, and the
 * final verification must still be done with the original select condition.
 * The basic outline of this rule is:
 * 1. Match operator pattern.
 * 2. Analyze select condition to see if there are optimizable functions (delegated to IAccessMethods).
 * 3. Check metadata to see if there are applicable indexes.
 * 4. Choose an index to apply (for now only a single index will be chosen).
 * 5. Rewrite plan using index (delegated to IAccessMethods).
 * If there are multiple secondary index access path available, we will use the intersection operator to get the
 * intersected primary key from all the secondary indexes. The detailed documentation is here
 * https://cwiki.apache.org/confluence/display/ASTERIXDB/Intersect+multiple+secondary+index
 */
public class IntroduceSelectAccessMethodRule extends AbstractIntroduceAccessMethodRule {

    // Operators representing the patterns to be matched:
    // These ops are set in matchesPattern()
    protected Mutable<ILogicalOperator> selectRef = null;
    protected SelectOperator selectOp = null;
    protected AbstractFunctionCallExpression selectCond = null;
    protected IVariableTypeEnvironment typeEnvironment = null;
    protected final OptimizableOperatorSubTree subTree = new OptimizableOperatorSubTree();
    protected List<Mutable<ILogicalOperator>> afterSelectRefs = null;

    // Register access methods.
    protected static Map<FunctionIdentifier, List<IAccessMethod>> accessMethods = new HashMap<>();

    static {
        registerAccessMethod(BTreeAccessMethod.INSTANCE, accessMethods);
        registerAccessMethod(RTreeAccessMethod.INSTANCE, accessMethods);
        registerAccessMethod(InvertedIndexAccessMethod.INSTANCE, accessMethods);
    }

    /**
     * Recursively check the given plan from the root operator to transform a plan
     * with SELECT operator into an index-utilized plan.
     */
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        clear();
        setMetadataDeclarations(context);

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // Already checked?
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        // We start at the top of the plan. Thus, check whether this operator is the root,
        // which is DISTRIBUTE_RESULT, SINK, or COMMIT since we start the process from the root operator.
        if (op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT
                && op.getOperatorTag() != LogicalOperatorTag.SINK
                && op.getOperatorTag() != LogicalOperatorTag.DELEGATE_OPERATOR) {
            return false;
        }

        if (op.getOperatorTag() == LogicalOperatorTag.DELEGATE_OPERATOR
                && !(((DelegateOperator) op).getDelegate() instanceof CommitOperator)) {
            return false;
        }

        afterSelectRefs = new ArrayList<>();
        // Recursively check the given plan whether the desired pattern exists in it.
        // If so, try to optimize the plan.
        boolean planTransformed = checkAndApplyTheSelectTransformation(opRef, context);

        if (selectOp != null) {
            // We found an optimization here. Don't need to optimize this operator again.
            context.addToDontApplySet(this, selectOp);
        }

        if (!planTransformed) {
            return false;
        } else {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }

        return planTransformed;
    }

    /**
     * Check that the given SELECT condition is a function call.
     * Call initSubTree() to initialize the optimizable subtree that collects information from
     * the operators below the given SELECT operator.
     * In order to transform the given plan, a datasource should be configured
     * since we are going to transform a datasource into an unnest-map operator.
     */
    protected boolean checkSelectOpConditionAndInitSubTree(IOptimizationContext context) throws AlgebricksException {
        // Set and analyze select.
        ILogicalExpression condExpr = selectOp.getCondition().getValue();
        typeEnvironment = context.getOutputTypeEnvironment(selectOp);
        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        selectCond = (AbstractFunctionCallExpression) condExpr;

        // Initialize the subtree information.
        // Match and put assign, unnest, and datasource information.
        boolean res = subTree.initFromSubTree(selectOp.getInputs().get(0));
        return res && subTree.hasDataSourceScan();
    }

    /**
     * Construct all applicable secondary index-based access paths in the given selection plan and
     * intersect them using INTERSECT operator to guide to the common primary index search.
     * In case where the applicable index is one, we only construct one path.
     */
    private boolean intersectAllSecondaryIndexes(List<Pair<IAccessMethod, Index>> chosenIndexes,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, IOptimizationContext context)
            throws AlgebricksException {
        Pair<IAccessMethod, Index> chosenIndex = null;
        Optional<Pair<IAccessMethod, Index>> primaryIndex =
                chosenIndexes.stream().filter(pair -> pair.second.isPrimaryIndex()).findFirst();
        if (chosenIndexes.size() == 1) {
            chosenIndex = chosenIndexes.get(0);
        } else if (primaryIndex.isPresent()) {
            // one primary + secondary indexes, choose the primary index directly.
            chosenIndex = primaryIndex.get();
        }
        if (chosenIndex != null) {
            AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(chosenIndex.first);
            return chosenIndex.first.applySelectPlanTransformation(afterSelectRefs, selectRef, subTree,
                    chosenIndex.second, analysisCtx, context);
        }

        // Intersect all secondary indexes, and postpone the primary index search.
        Mutable<ILogicalExpression> conditionRef = selectOp.getCondition();

        List<ILogicalOperator> subRoots = new ArrayList<>();
        for (Pair<IAccessMethod, Index> pair : chosenIndexes) {
            AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(pair.first);
            subRoots.add(pair.first.createSecondaryToPrimaryPlan(conditionRef, subTree, null, pair.second, analysisCtx,
                    AccessMethodUtils.retainInputs(subTree.getDataSourceVariables(),
                            subTree.getDataSourceRef().getValue(), afterSelectRefs),
                    false, subTree.getDataSourceRef().getValue().getInputs().get(0).getValue()
                            .getExecutionMode() == ExecutionMode.UNPARTITIONED,
                    context));
        }
        // Connect each secondary index utilization plan to a common intersect operator.
        ILogicalOperator primaryUnnestOp = connectAll2ndarySearchPlanWithIntersect(subRoots, context);

        subTree.getDataSourceRef().setValue(primaryUnnestOp);
        return primaryUnnestOp != null;
    }

    /**
     * Connect each secondary index utilization plan to a common INTERSECT operator.
     */
    private ILogicalOperator connectAll2ndarySearchPlanWithIntersect(List<ILogicalOperator> subRoots,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator lop = subRoots.get(0);
        List<List<LogicalVariable>> inputVars = new ArrayList<>(subRoots.size());
        for (int i = 0; i < subRoots.size(); i++) {
            if (lop.getOperatorTag() != subRoots.get(i).getOperatorTag()) {
                throw new AlgebricksException("The data source root should have the same operator type.");
            }
            if (lop.getInputs().size() != 1) {
                throw new AlgebricksException("The primary search has multiple inputs.");
            }

            ILogicalOperator curRoot = subRoots.get(i);
            OrderOperator order = (OrderOperator) curRoot.getInputs().get(0).getValue();
            List<LogicalVariable> orderedColumn = new ArrayList<>(order.getOrderExpressions().size());
            for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> orderExpression : order
                    .getOrderExpressions()) {
                if (orderExpression.second.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                    throw new AlgebricksException(
                            "The order by expression should be variables, but they aren't variables.");
                }
                VariableReferenceExpression orderedVar =
                        (VariableReferenceExpression) orderExpression.second.getValue();
                orderedColumn.add(orderedVar.getVariableReference());
            }
            inputVars.add(orderedColumn);
        }

        List<LogicalVariable> outputVar = inputVars.get(0);
        IntersectOperator intersect = new IntersectOperator(outputVar, inputVars);
        for (ILogicalOperator secondarySearch : subRoots) {
            intersect.getInputs().add(secondarySearch.getInputs().get(0));
        }
        context.computeAndSetTypeEnvironmentForOperator(intersect);
        lop.getInputs().set(0, new MutableObject<>(intersect));
        return lop;
    }

    /**
     * Recursively traverse the given plan and check whether a SELECT operator exists.
     * If one is found, maintain the path from the root to SELECT operator and
     * optimize the path from the SELECT operator to the EMPTY_TUPLE_SOURCE operator
     * if it is not already optimized.
     */
    protected boolean checkAndApplyTheSelectTransformation(Mutable<ILogicalOperator> opRef,
            IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        boolean selectFoundAndOptimizationApplied;
        boolean isSelectOp = false;

        Mutable<ILogicalOperator> selectRefFromThisOp = null;
        SelectOperator selectOpFromThisOp = null;

        // Check the current operator pattern to see whether it is a JOIN or not.
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            selectRef = opRef;
            selectOp = (SelectOperator) op;
            selectRefFromThisOp = opRef;
            selectOpFromThisOp = (SelectOperator) op;
            isSelectOp = true;
        } else {
            // This is not a SELECT operator. Remember this operator.
            afterSelectRefs.add(opRef);
        }

        // Recursively check the plan and try to optimize it. We first check the children of the given operator
        // to make sure an earlier select in the path is optimized first.
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            selectFoundAndOptimizationApplied = checkAndApplyTheSelectTransformation(inputOpRef, context);
            if (selectFoundAndOptimizationApplied) {
                return true;
            }
        }

        // Traverse the plan until we find a SELECT operator.
        if (isSelectOp) {
            // Restore the information from this operator since it might have been be set to null
            // if there are other select operators in the earlier path.
            selectRef = selectRefFromThisOp;
            selectOp = selectOpFromThisOp;

            // Decides the plan transformation check needs to be continued.
            // This variable is needed since we can't just return false
            // in order to keep this operator in the afterSelectRefs list.
            boolean continueCheck = true;

            // Already checked this SELECT operator? If not, this operator may be optimized.
            if (context.checkIfInDontApplySet(this, selectOp)) {
                continueCheck = false;
            }

            // For each access method, contains the information about
            // whether an available index can be applicable or not.
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = null;
            if (continueCheck) {
                analyzedAMs = new TreeMap<>();
            }

            // Check the condition of SELECT operator is a function call since
            // only function call can be transformed using available indexes.
            // If so, initialize the subtree information that will be used later to decide whether
            // the given plan is truly optimizable or not.
            if (continueCheck && !checkSelectOpConditionAndInitSubTree(context)) {
                continueCheck = false;
            }

            // Analyze the condition of SELECT operator and initialize analyzedAMs.
            // Check whether the function in the SELECT operator can be truly transformed.
            if (continueCheck && !analyzeSelectOrJoinOpConditionAndUpdateAnalyzedAM(selectCond,
                    subTree.getAssignsAndUnnests(), analyzedAMs, context, typeEnvironment)) {
                continueCheck = false;
            }

            // Find the dataset from the data-source and
            // the record type of the dataset from the metadata.
            // This will be used to find an applicable index on the dataset.
            if (continueCheck && !subTree.setDatasetAndTypeMetadata((MetadataProvider) context.getMetadataProvider())) {
                continueCheck = false;
            }

            if (continueCheck) {
                // Map variables to the applicable indexes and find the field name and type.
                // Then find the applicable indexes for the variables used in the SELECT condition.
                fillSubTreeIndexExprs(subTree, analyzedAMs, context);

                // Prune the access methods based on the function expression and access methods.
                pruneIndexCandidates(analyzedAMs, context, typeEnvironment);

                // Choose all indexes that will be applied.
                List<Pair<IAccessMethod, Index>> chosenIndexes = chooseAllIndexes(analyzedAMs);

                if (chosenIndexes == null || chosenIndexes.isEmpty()) {
                    // We can't apply any index for this SELECT operator
                    context.addToDontApplySet(this, selectRef.getValue());
                    return false;
                }

                // Apply plan transformation using chosen index.
                boolean res = intersectAllSecondaryIndexes(chosenIndexes, analyzedAMs, context);
                context.addToDontApplySet(this, selectOp);

                if (res) {
                    OperatorPropertiesUtil.typeOpRec(opRef, context);
                    return res;
                }
            }

            selectRef = null;
            selectOp = null;
            afterSelectRefs.add(opRef);
        }

        // Clean the path after SELECT operator by removing the current operator in the list.
        afterSelectRefs.remove(opRef);

        return false;

    }

    @Override
    public Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods() {
        return accessMethods;
    }

    private void clear() {
        afterSelectRefs = null;
        selectRef = null;
        selectOp = null;
        selectCond = null;
        subTree.reset();
    }
}
