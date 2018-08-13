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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * This rule optimizes a join with secondary indexes into an indexed nested-loop join.
 * This rule matches the following operator pattern:
 * join <-- select? <-- (assign|unnest)+ <-- (datasource scan|unnest-map)
 * The order of the join inputs matters (left branch - outer relation, right branch - inner relation).
 * This rule tries to utilize an index on the inner relation.
 * If that's not possible, it stops transforming the given join into an index-nested-loop join.
 *
 * This rule replaces the above pattern with the following simplified plan:
 * select <-- assign+ <-- unnest-map(pidx) <-- sort <-- unnest-map(sidx) <-- assign+ <-- (datasource scan|unnest-map)
 * The sorting PK process is optional, and some access methods may choose not to sort.
 * Note that for some index-based optimizations we do not remove the triggering
 * condition from the join, since the secondary index may only act as a filter, and the
 * final verification must still be done with the original join condition.
 *
 * The basic outline of this rule is:
 * 1. Match operator pattern.
 * 2. Analyze join condition to see if there are optimizable functions (delegated to IAccessMethods).
 * 3. Check metadata to see if there are applicable indexes.
 * 4. Choose an index to apply (for now only a single index will be chosen).
 * 5. Rewrite plan using index (delegated to IAccessMethods).
 *
 * For left-outer-join, additional patterns are checked and additional treatment is needed as follows:
 * 1. First it checks if there is a groupByOp above the join: groupby <-- leftouterjoin
 * 2. Inherently, only the right-subtree of the lojOp can be used as indexSubtree.
 * So, the right-subtree must have at least one applicable index on join field(s).
 * 3. If there is a groupByOp, the null placeholder variable introduced in groupByOp should be taken care of correctly.
 * Here, the primary key variable from datasourceScanOp replaces the introduced null placeholder variable.
 * If the primary key is a composite key, then the first variable of the primary key variables becomes the
 * null place holder variable. This null placeholder variable works for all three types of indexes.
 *
 * If the inner-branch can be transformed as an index-only plan, this rule creates an index-only-plan path
 * that is similar to one described in IntroduceSelectAccessMethod Rule.
 */
public class IntroduceJoinAccessMethodRule extends AbstractIntroduceAccessMethodRule {

    protected Mutable<ILogicalOperator> joinRef = null;
    protected AbstractBinaryJoinOperator joinOp = null;
    protected AbstractFunctionCallExpression joinCond = null;
    protected final OptimizableOperatorSubTree leftSubTree = new OptimizableOperatorSubTree();
    protected final OptimizableOperatorSubTree rightSubTree = new OptimizableOperatorSubTree();
    protected IVariableTypeEnvironment typeEnvironment = null;
    protected boolean hasGroupBy = true;
    protected List<Mutable<ILogicalOperator>> afterJoinRefs = null;

    // Registers access methods.
    protected static Map<FunctionIdentifier, List<IAccessMethod>> accessMethods = new HashMap<>();

    static {
        registerAccessMethod(BTreeAccessMethod.INSTANCE, accessMethods);
        registerAccessMethod(RTreeAccessMethod.INSTANCE, accessMethods);
        registerAccessMethod(InvertedIndexAccessMethod.INSTANCE, accessMethods);
    }

    /**
     * Recursively checks the given plan from the root operator to transform a plan
     * with INNERJOIN or LEFT-OUTER-JOIN operator into an index-utilized plan.
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

        // Checks whether this operator is the root, which is DISTRIBUTE_RESULT or SINK since
        // we start the process from the root operator.
        if (op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT
                && op.getOperatorTag() != LogicalOperatorTag.SINK
                && op.getOperatorTag() != LogicalOperatorTag.DELEGATE_OPERATOR) {
            return false;
        }

        afterJoinRefs = new ArrayList<>();
        // Recursively checks the given plan whether the desired pattern exists in it.
        // If so, try to optimize the plan.
        boolean planTransformed = checkAndApplyJoinTransformation(opRef, context);

        if (joinOp != null) {
            // We found an optimization here. Don't need to optimize this operator again.
            context.addToDontApplySet(this, joinOp);
        }

        if (!planTransformed) {
            return false;
        } else {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }

        return planTransformed;
    }

    /**
     * Removes indexes from the outer branch from the optimizer's consideration for this rule,
     * since we only use indexes from the inner branch.
     */
    protected void pruneIndexCandidatesFromOuterBranch(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        // Inner branch is the right side branch of the given JOIN operator.
        String innerDataset = null;
        if (rightSubTree.getDataset() != null) {
            innerDataset = rightSubTree.getDataset().getDatasetName();
        }

        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> entry = amIt.next();
            AccessMethodAnalysisContext amCtx = entry.getValue();

            // Fetch index, expression, and variables.
            Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexIt = amCtx.getIteratorForIndexExprsAndVars();

            while (indexIt.hasNext()) {
                Map.Entry<Index, List<Pair<Integer, Integer>>> indexExprAndVarEntry = indexIt.next();
                Iterator<Pair<Integer, Integer>> exprsAndVarIter = indexExprAndVarEntry.getValue().iterator();
                boolean indexFromInnerBranch = false;

                while (exprsAndVarIter.hasNext()) {
                    Pair<Integer, Integer> exprAndVarIdx = exprsAndVarIter.next();
                    IOptimizableFuncExpr optFuncExpr = amCtx.getMatchedFuncExpr(exprAndVarIdx.first);

                    // We check the dataset name and the subtree to make sure
                    // that this index come from the inner branch.
                    if (indexExprAndVarEntry.getKey().getDatasetName().equals(innerDataset)) {
                        if (optFuncExpr.getOperatorSubTree(exprAndVarIdx.second).equals(rightSubTree)) {
                            indexFromInnerBranch = true;
                        }
                    }
                }

                // If the given index does not come from the inner branch,
                // prune this index so that the optimizer doesn't consider this index in this rule.
                if (!indexFromInnerBranch) {
                    indexIt.remove();
                }
            }
        }
    }

    /**
     * Checks whether the given operator is LEFTOUTERJOIN.
     * If so, also checks that GROUPBY is placed after LEFTOUTERJOIN.
     */
    // Check whether (Groupby)? <-- Leftouterjoin
    private boolean isLeftOuterJoin(AbstractLogicalOperator op1) {
        if (op1.getInputs().size() != 1) {
            return false;
        }
        if (op1.getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        if (op1.getOperatorTag() == LogicalOperatorTag.GROUP) {
            return true;
        }
        hasGroupBy = false;
        return true;
    }

    /**
     * Checks whether the given operator is INNERJOIN.
     */
    private boolean isInnerJoin(AbstractLogicalOperator op1) {
        return op1.getOperatorTag() == LogicalOperatorTag.INNERJOIN;
    }

    @Override
    public Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods() {
        return accessMethods;
    }

    private void clear() {
        joinRef = null;
        joinOp = null;
        joinCond = null;
        afterJoinRefs = null;
    }

    /**
     * Recursively traverses the given plan and check whether a INNERJOIN or LEFTOUTERJOIN operator exists.
     * If one is found, maintain the path from the root to the given join operator and
     * optimize the path from the given join operator to the EMPTY_TUPLE_SOURCE operator
     * if it is not already optimized.
     */
    protected boolean checkAndApplyJoinTransformation(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        boolean joinFoundAndOptimizationApplied;

        // Adds the current operator to the operator list that contains operators after a join operator
        // in case there is a descendant join operator and it could be transformed first.
        afterJoinRefs.add(opRef);

        // Recursively check the plan and try to optimize it. We first check the children of the given operator
        // to make sure an earlier join in the path is optimized first.
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            joinFoundAndOptimizationApplied = checkAndApplyJoinTransformation(inputOpRef, context);
            if (joinFoundAndOptimizationApplied) {
                return true;
            }
        }

        // Now, we are sure that transformation attempts for earlier joins have been failed.
        // Checks the current operator pattern to see whether it is a JOIN or not.
        boolean isThisOpInnerJoin = isInnerJoin(op);
        boolean isThisOpLeftOuterJoin = isLeftOuterJoin(op);
        boolean isParentOpGroupBy = hasGroupBy;

        Mutable<ILogicalOperator> joinRefFromThisOp = null;
        AbstractBinaryJoinOperator joinOpFromThisOp = null;
        // operators that need to be removed from the afterJoinRefs list.
        Mutable<ILogicalOperator> opRefRemove = opRef;
        if (isThisOpInnerJoin) {
            // Sets the join operator.
            joinRef = opRef;
            joinOp = (InnerJoinOperator) op;
            joinRefFromThisOp = opRef;
            joinOpFromThisOp = (InnerJoinOperator) op;
        } else if (isThisOpLeftOuterJoin) {
            // Sets the left-outer-join operator.
            // The current operator is GROUP and the child of this operator is LEFTOUERJOIN.
            joinRef = op.getInputs().get(0);
            joinOp = (LeftOuterJoinOperator) joinRef.getValue();
            joinRefFromThisOp = op.getInputs().get(0);
            joinOpFromThisOp = (LeftOuterJoinOperator) joinRefFromThisOp.getValue();

            // Group-by should not be removed at this point since the given left-outer-join can be transformed.
            opRefRemove = op.getInputs().get(0);
        }
        afterJoinRefs.remove(opRefRemove);

        // For a JOIN case, tries to transform the given plan.
        if (isThisOpInnerJoin || isThisOpLeftOuterJoin) {

            // Restores the information from this operator since it might have been be set to null
            // if there are other join operators in the earlier path.
            joinRef = joinRefFromThisOp;
            joinOp = joinOpFromThisOp;

            boolean continueCheck = true;

            // Already checked? If not, this operator may be optimized.
            if (context.checkIfInDontApplySet(this, joinOp)) {
                continueCheck = false;
            }

            // For each access method, this contains the information about
            // whether an available index can be applicable or not.
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = null;
            if (continueCheck) {
                analyzedAMs = new HashMap<>();
            }

            // Checks the condition of JOIN operator is a function call since only function call can be transformed
            // using available indexes. If so, initializes the subtree information that will be used later to decide
            // whether the given plan is truly optimizable or not.
            if (continueCheck && !checkJoinOpConditionAndInitSubTree(context)) {
                continueCheck = false;
            }

            // Analyzes the condition of SELECT operator and initializes analyzedAMs.
            // Check whether the function in the SELECT operator can be truly transformed.
            boolean matchInLeftSubTree = false;
            boolean matchInRightSubTree = false;
            if (continueCheck) {
                if (leftSubTree.hasDataSource()) {
                    matchInLeftSubTree = analyzeSelectOrJoinOpConditionAndUpdateAnalyzedAM(joinCond,
                            leftSubTree.getAssignsAndUnnests(), analyzedAMs, context, typeEnvironment);
                }
                if (rightSubTree.hasDataSource()) {
                    matchInRightSubTree = analyzeSelectOrJoinOpConditionAndUpdateAnalyzedAM(joinCond,
                            rightSubTree.getAssignsAndUnnests(), analyzedAMs, context, typeEnvironment);
                }
            }

            // Finds the dataset from the data-source and the record type of the dataset from the metadata.
            // This will be used to find an applicable index on the dataset.
            boolean checkLeftSubTreeMetadata = false;
            boolean checkRightSubTreeMetadata = false;
            if (continueCheck && matchInRightSubTree) {
                // Set dataset and type metadata.
                if (matchInLeftSubTree) {
                    checkLeftSubTreeMetadata = leftSubTree.setDatasetAndTypeMetadata(metadataProvider);
                }
                checkRightSubTreeMetadata = rightSubTree.setDatasetAndTypeMetadata(metadataProvider);
            }

            if (continueCheck && checkRightSubTreeMetadata) {
                // Map variables to the applicable indexes and find the field name and type.
                // Then find the applicable indexes for the variables used in the JOIN condition.
                if (checkLeftSubTreeMetadata) {
                    fillSubTreeIndexExprs(leftSubTree, analyzedAMs, context);
                } else {
                    fillSubTreeIndexExprs(leftSubTree, analyzedAMs, context, true);
                }
                fillSubTreeIndexExprs(rightSubTree, analyzedAMs, context);

                // Prunes the access methods based on the function expression and access methods.
                pruneIndexCandidates(analyzedAMs, context, typeEnvironment);

                // If the right subtree (inner branch) has indexes, one of those indexes will be used.
                // Removes the indexes from the outer branch in the optimizer's consideration list for this rule.
                pruneIndexCandidatesFromOuterBranch(analyzedAMs);

                // We are going to use indexes from the inner branch.
                // If no index is available, then we stop here.
                Pair<IAccessMethod, Index> chosenIndex = chooseBestIndex(analyzedAMs);
                if (chosenIndex == null) {
                    context.addToDontApplySet(this, joinOp);
                    continueCheck = false;
                }

                if (continueCheck) {
                    // Finds the field name of each variable in the sub-tree such as variables for order by.
                    // This step is required when checking index-only plan.
                    if (checkLeftSubTreeMetadata) {
                        fillFieldNamesInTheSubTree(leftSubTree);
                    }
                    if (checkRightSubTreeMetadata) {
                        fillFieldNamesInTheSubTree(rightSubTree);
                    }

                    // Applies the plan transformation using chosen index.
                    AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(chosenIndex.first);

                    // For LOJ with GroupBy, prepare objects to reset LOJ nullPlaceHolderVariable
                    // in GroupByOp.
                    if (isThisOpLeftOuterJoin && isParentOpGroupBy) {
                        analysisCtx.setLOJGroupbyOpRef(opRef);
                        ScalarFunctionCallExpression isNullFuncExpr = AccessMethodUtils
                                .findLOJIsMissingFuncInGroupBy((GroupByOperator) opRef.getValue(), rightSubTree);
                        analysisCtx.setLOJIsMissingFuncInGroupBy(isNullFuncExpr);
                    }

                    Dataset indexDataset = analysisCtx.getDatasetFromIndexDatasetMap(chosenIndex.second);

                    // We assume that the left subtree is the outer branch and the right subtree
                    // is the inner branch. This assumption holds true since we only use an index
                    // from the right subtree. The following is just a sanity check.
                    if (!rightSubTree.hasDataSourceScan()
                            && !indexDataset.getDatasetName().equals(rightSubTree.getDataset().getDatasetName())) {
                        return false;
                    }

                    // Finally, tries to apply plan transformation using the chosen index.
                    boolean res = chosenIndex.first.applyJoinPlanTransformation(afterJoinRefs, joinRef, leftSubTree,
                            rightSubTree, chosenIndex.second, analysisCtx, context, isThisOpLeftOuterJoin,
                            isParentOpGroupBy);

                    // If the plan transformation is successful, we don't need to traverse the plan
                    // any more, since if there are more JOIN operators, the next trigger on this plan
                    // will find them.
                    if (res) {
                        return res;
                    }
                }
            }

            joinRef = null;
            joinOp = null;
        }

        // Checked the given left-outer-join operator and it is not transformed. So, this group-by operator
        // after the left-outer-join operator should be removed from the afterJoinRefs list
        // since the current operator is a group-by operator.
        if (isThisOpLeftOuterJoin) {
            afterJoinRefs.remove(opRef);
        }

        return false;
    }

    /**
     * After the pattern is matched, checks the condition and initializes the data source
     * from the right (inner) sub tree.
     *
     * @throws AlgebricksException
     */
    protected boolean checkJoinOpConditionAndInitSubTree(IOptimizationContext context) throws AlgebricksException {

        typeEnvironment = context.getOutputTypeEnvironment(joinOp);

        // Check that the join's condition is a function call.
        ILogicalExpression condExpr = joinOp.getCondition().getValue();
        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        joinCond = (AbstractFunctionCallExpression) condExpr;

        // The result of the left subtree initialization does not need to be checked since only the field type
        // of the field that is being joined is important. However, if we do not initialize the left sub tree,
        // we lose a chance to get the field type of a field if there is an enforced index on it.
        leftSubTree.initFromSubTree(joinOp.getInputs().get(0));
        boolean rightSubTreeInitialized = rightSubTree.initFromSubTree(joinOp.getInputs().get(1));

        if (!rightSubTreeInitialized) {
            return false;
        }

        // The right (inner) subtree must have a datasource scan.
        if (rightSubTree.hasDataSourceScan()) {
            return true;
        }
        return false;
    }

}
