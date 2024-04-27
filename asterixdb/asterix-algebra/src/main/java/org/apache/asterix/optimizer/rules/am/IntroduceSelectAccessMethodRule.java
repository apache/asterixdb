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
import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.optimizer.rules.am.array.IIntroduceAccessMethodRuleLocalRewrite;
import org.apache.asterix.optimizer.rules.am.array.MergedSelectRewrite;
import org.apache.asterix.optimizer.rules.am.array.SelectFromSubplanRewrite;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * This rule optimizes simple selections with secondary or primary indexes.
 * The use of an index is expressed as an UNNESTMAP operator over an index-search function which will be
 * replaced with the appropriate embodiment during codegen.
 * This rule seeks to change the following patterns.
 * For the secondary-index searches, a SELECT operator is followed by one or more ASSIGN / UNNEST operators.
 * A DATASOURCE_SCAN operator should be placed before these operators.
 * For the primary-index search, a SELECT operator is followed by DATASOURE_SCAN operator since no ASSIGN / UNNEST
 * operator is required to get the primary key fields (they are already stored fields in the BTree tuples).
 * If the above pattern is found, this rule replaces the pattern with the following pattern.
 * If the given plan is both a secondary-index search and an index-only plan, it builds two paths.
 * The left path has a UNIONALL operator at the top. And the original SELECT operator is followed. Also, the original
 * ASSIGN / UNNEST operators are followed. Then, UNNEST-MAP for the primary-index-search is followed
 * to fetch the record. Before that, a SPLIT operator is introduced. Before this, an UNNEST-MAP for
 * the secondary-index-search is followed to conduct a secondary-index search. The search key (original ASSIGN/UNNEST)
 * to the secondary-index-search (UNNEST-MAP) is placed before that.
 * The right path has the above UNIONALL operator at the top. Then, possibly has optional SELECT and/or ASSIGN/UNNEST
 * operators for the composite BTree or RTree search cases. Then, the above SPLIT operator is followed. Before the SPLIT
 * operator, it shares the same operators with the left path.
 * To be qualified as an index-only plan, there are two conditions.
 * 1) Search predicate can be covered by a secondary index-search.
 * 2) there are only PK and/or SK fields in the return clause.
 * If the given query satisfies the above conditions, we call it an index-only plan.
 * The benefit of the index-only plan is that we don't need to traverse the primary index
 * after fetching SK, PK entries from a secondary index.
 * The index-only plan works as follows.
 * 1) During a secondary-index search, after fetching <SK, PK> pair that satisfies the given predicate,
 * we try to get an instantTryLock on PK to verify that <SK, PK> is a valid pair.
 * If it succeeds, the given <SK, PK> pair is trustworthy so that we can return this as a valid output.
 * This tuple goes to the right path of UNIONALL operator since we don't need to traverse the primary index.
 * If instantTryLock on PK fails, an operation on the PK record is ongoing, so we need to traverse
 * the primary index to fetch the entire record and verify the search predicate. So, this <SK, PK> pair
 * goes to the left path of UNIONALL operator to traverse the primary index.
 * In the left path, we fetch the record using the given PK and fetch SK field and does SELECT verification.
 * 2) A UNIONALL operator combines tuples from the left path and the right path and the rest of the plan continues.
 * In an index-only plan, sort before the primary index-search is not required since we assume that
 * the chances that a tuple (<SK, PK> pair) goes into the left path are low.
 * If the given query plan is not an index-only plan, we call this plan as non-index only plan.
 * In this case, the original plan will be transformed into the following pattern.
 * The original SELECT operator is placed at the top. And the original ASSIGN / UNNEST operators are followed.
 * An UNNEST-MAP that conducts the primary-index-search to fetch the primary keys are placed before that. An ORDER
 * operator is placed to sort the primary keys before feed them into the primary-index. Then, an UNNEST-MAP is followed
 * to conduct a secondary-index search. Then, the search key (ASSIGN / UNNEST) is followed.
 * In this case, the sort is optional, and some access methods implementations may choose not to sort.
 * Note that for some index-based optimizations we do not remove the triggering
 * condition from the select, since the index may only acts as a filter, and the
 * final verification must still be done with the original select condition.
 * The basic outline of this rule is:
 * 1. Match operator pattern.
 * 2. Analyze select condition to see if there are optimizable functions (delegated to IAccessMethods).
 * 3. Check meta-data to see if there are applicable indexes.
 * 4. Choose an index (or more indexes) to apply.
 * 5. Rewrite the plan using index(es) (delegated to IAccessMethods).
 * If multiple secondary index access paths are available, the optimizer uses the intersection operator
 * to get the intersected primary key from all the chosen secondary indexes. In this case, we don't check
 * whether the given plan is an index-only plan.
 * The detailed documentation of intersecting multiple secondary indexes is here:
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

    // For plan rewriting to recognize applicable array indexes.
    private final SelectFromSubplanRewrite selectFromSubplanRewrite = new SelectFromSubplanRewrite();
    private final MergedSelectRewrite mergedSelectRewrite = new MergedSelectRewrite();

    // Register access methods.
    protected static Map<FunctionIdentifier, List<IAccessMethod>> accessMethods = new HashMap<>();

    static {
        registerAccessMethod(BTreeAccessMethod.INSTANCE, accessMethods);
        registerAccessMethod(RTreeAccessMethod.INSTANCE, accessMethods);
        registerAccessMethod(InvertedIndexAccessMethod.INSTANCE, accessMethods);
        registerAccessMethod(ArrayBTreeAccessMethod.INSTANCE, accessMethods);
        for (Pair<FunctionIdentifier, Boolean> f : ArrayBTreeAccessMethod.INSTANCE.getOptimizableFunctions()) {
            SelectFromSubplanRewrite.addOptimizableFunction(f.first);
        }
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
        List<Pair<IAccessMethod, Index>> chosenIndexes = new ArrayList<>();
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = null;
        boolean planTransformed =
                checkAndApplyTheSelectTransformation(opRef, context, false, chosenIndexes, analyzedAMs);

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

    public boolean checkApplicable(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            List<Pair<IAccessMethod, Index>> chosenIndexes, Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs)
            throws AlgebricksException {
        clear();
        setMetadataDeclarations(context);

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // Already checked?
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        afterSelectRefs = new ArrayList<>();
        // Recursively check the given plan whether the desired pattern exists in it.
        // If so, try to optimize the plan.

        boolean planTransformed =
                checkAndApplyTheSelectTransformation(opRef, context, true, chosenIndexes, analyzedAMs);

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
     * Constructs all applicable secondary index-based access paths in the given selection plan and
     * intersects them using INTERSECT operator to guide to the common primary-index search.
     * This method assumes that there are two or more secondary indexes in the given path.
     */
    private boolean intersectAllSecondaryIndexes(List<Pair<IAccessMethod, Index>> chosenIndexes,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, IOptimizationContext context)
            throws AlgebricksException {
        if (chosenIndexes.size() == 1) {
            throw CompilationException.create(ErrorCode.CHOSEN_INDEX_COUNT_SHOULD_BE_GREATER_THAN_ONE);
        }

        // Intersect all secondary indexes, and postpone the primary index search.
        Mutable<ILogicalExpression> conditionRef = selectOp.getCondition();

        List<ILogicalOperator> subRoots = new ArrayList<>();
        for (Pair<IAccessMethod, Index> pair : chosenIndexes) {
            AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(pair.first);
            boolean retainInput = AccessMethodUtils.retainInputs(subTree.getDataSourceVariables(),
                    subTree.getDataSourceRef().getValue(), afterSelectRefs);
            boolean requiresBroadcast = subTree.getDataSourceRef().getValue().getInputs().get(0).getValue()
                    .getExecutionMode() == ExecutionMode.UNPARTITIONED;
            ILogicalOperator subRoot = pair.first.createIndexSearchPlan(afterSelectRefs, selectRef, conditionRef,
                    subTree.getAssignsAndUnnestsRefs(), subTree, null, pair.second, analysisCtx, retainInput, false,
                    requiresBroadcast, context, null, null);
            if (subRoot == null) {
                return false;
            }
            subRoots.add(subRoot);
        }
        // Connect each secondary index utilization plan to a common intersect operator.
        Index idx = chosenIndexes.get(0).getSecond();
        ILogicalOperator primaryUnnestOp = connectAll2ndarySearchPlanWithIntersect(subRoots, context, idx);

        subTree.getDataSourceRef().setValue(primaryUnnestOp);
        return primaryUnnestOp != null;
    }

    /**
     * Checks whether the primary index exists among the applicable indexes and return it if is exists.
     *
     * @param chosenIndexes
     * @return Pair<IAccessMethod, Index> for the primary index
     * null otherwise
     * @throws AlgebricksException
     */
    private Pair<IAccessMethod, Index> fetchPrimaryIndexAmongChosenIndexes(
            List<Pair<IAccessMethod, Index>> chosenIndexes) throws AlgebricksException {
        Optional<Pair<IAccessMethod, Index>> primaryIndex =
                chosenIndexes.stream().filter(pair -> pair.second.isPrimaryIndex()).findFirst();
        if (primaryIndex.isPresent()) {
            return primaryIndex.get();
        }
        return null;
    }

    /**
     * Connect each secondary index utilization plan to a common INTERSECT operator.
     */
    private ILogicalOperator connectAll2ndarySearchPlanWithIntersect(List<ILogicalOperator> subRoots,
            IOptimizationContext context, Index idx) throws AlgebricksException {
        ILogicalOperator lop = subRoots.get(0);
        List<List<LogicalVariable>> inputVars = new ArrayList<>(subRoots.size());
        for (int i = 0; i < subRoots.size(); i++) {
            if (lop.getOperatorTag() != subRoots.get(i).getOperatorTag()) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, lop.getSourceLocation(),
                        "The data source root should have the same operator type.");
            }
            if (lop.getInputs().size() != 1) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, lop.getSourceLocation(),
                        "The primary search has multiple inputs.");
            }

            // The operator directly under the subroot may either be an ORDER-BY or a DISTINCT (if an array index).
            ILogicalOperator curRoot = subRoots.get(i);
            ILogicalOperator curRootInput = curRoot.getInputs().get(0).getValue();
            if (curRootInput.getOperatorTag() == LogicalOperatorTag.ORDER) {
                OrderOperator order = (OrderOperator) curRootInput;
                List<LogicalVariable> orderedColumn = new ArrayList<>(order.getOrderExpressions().size());
                for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> orderExpression : order
                        .getOrderExpressions()) {
                    if (orderExpression.second.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                                orderExpression.second.getValue().getSourceLocation(),
                                "The order by expression should be variables, but they aren't variables.");
                    }
                    VariableReferenceExpression orderedVar =
                            (VariableReferenceExpression) orderExpression.second.getValue();
                    orderedColumn.add(orderedVar.getVariableReference());
                }
                inputVars.add(orderedColumn);
            } else if (curRootInput.getOperatorTag() == LogicalOperatorTag.DISTINCT) {
                DistinctOperator distinct = (DistinctOperator) curRootInput;
                inputVars.add(distinct.getDistinctByVarList());
            } else {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, curRootInput.getSourceLocation(),
                        "Operator directly below the primary index search should be either a DISTINCT or an ORDER!");
            }
        }

        List<LogicalVariable> inputVars0 = inputVars.get(0);
        List<LogicalVariable> outputVars = new ArrayList<>(inputVars0.size());
        for (LogicalVariable inputVar : inputVars0) {
            LogicalVariable outputVar = context.newVar();
            outputVars.add(outputVar);
            VariableUtilities.substituteVariables(lop, inputVar, outputVar, context);
        }
        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(idx);
        IntersectOperator intersect =
                new IntersectOperator(outputVars, inputVars, partitioningProperties.getComputeStorageMap());
        intersect.setSourceLocation(lop.getSourceLocation());
        for (ILogicalOperator secondarySearch : subRoots) {
            intersect.getInputs().add(secondarySearch.getInputs().get(0));
        }
        context.computeAndSetTypeEnvironmentForOperator(intersect);
        lop.getInputs().set(0, new MutableObject<>(intersect));
        return lop;
    }

    // list1 is <= list2 in terms of size; so check if everything in list1 is also in list2 in the same order
    protected boolean prefix(List<List<String>> list1, List<List<String>> list2) {
        int i, j;

        for (i = 0; i < list1.size(); i++) {
            List<String> l1 = list1.get(i);
            List<String> l2 = list2.get(i);
            if (l1.size() != l2.size()) {
                return false;
            }
            for (j = 0; j < l1.size(); j++) {
                String s1 = l1.get(j);
                String s2 = l2.get(j);
                if (!(s1.equals(s2))) {
                    return false;
                }
            }
        }
        return true;
    }

    protected void removeSmallerPrefixIndexes(List<Pair<IAccessMethod, Index>> indexes) throws CompilationException {
        int len = indexes.size();
        int i, j;
        Index indexI, indexJ;
        boolean include[];
        include = new boolean[len];
        for (i = 0; i < len; i++) {
            include[i] = true; // Initially every index is included.
        }

        List<List<String>> fieldNamesI, fieldNamesJ;

        for (i = 0; i < len - 1; i++) {
            if (include[i]) {
                IAccessMethod ami = indexes.get(i).first;
                indexI = indexes.get(i).second;
                DatasetConfig.IndexType typeI = indexI.getIndexType();
                fieldNamesI = findKeyFieldNames(indexI);

                for (j = i + 1; j < len; j++) {
                    if (include[j]) {
                        IAccessMethod amj = indexes.get(j).first;
                        if (ami == amj) { // should be the same accessMethods
                            indexJ = indexes.get(j).second;
                            DatasetConfig.IndexType typeJ = indexJ.getIndexType();
                            if (typeI == typeJ) {
                                fieldNamesJ = findKeyFieldNames(indexJ);
                                if (fieldNamesI.size() <= fieldNamesJ.size()) {
                                    if (prefix(fieldNamesI, fieldNamesJ)) {
                                        include[i] = false;
                                    }
                                } else if (prefix(fieldNamesJ, fieldNamesI)) {
                                    include[j] = false;
                                }
                            }
                        }
                    }
                }
            }
        }

        // remove the shorter indexes if any
        for (i = len - 1; i >= 0; i--) { // removing from the end; seems safer that way
            if (!include[i]) { // if this index can be removed it, do so;
                indexes.remove(i);
            }
        }
    }

    /**
     * Recursively traverse the given plan and check whether a SELECT operator exists.
     * If one is found, maintain the path from the root to SELECT operator and
     * optimize the path from the SELECT operator to the EMPTY_TUPLE_SOURCE operator
     * if it is not already optimized.
     */
    protected boolean checkAndApplyTheSelectTransformation(Mutable<ILogicalOperator> opRef,
            IOptimizationContext context, boolean checkApplicableOnly, List<Pair<IAccessMethod, Index>> chosenIndexes,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        boolean selectFoundAndOptimizationApplied;
        boolean isSelectOp = false;

        IPlanPrettyPrinter pp = context.getPrettyPrinter();
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
            selectFoundAndOptimizationApplied = checkAndApplyTheSelectTransformation(inputOpRef, context,
                    checkApplicableOnly, chosenIndexes, analyzedAMs);
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
            if (!checkApplicableOnly && continueCheck) {
                analyzedAMs = new TreeMap<>();
            }

            if (continueCheck && context.getPhysicalOptimizationConfig().isArrayIndexEnabled()
                    && SelectFromSubplanRewrite.isApplicableForRewriteCursory(metadataProvider, selectOp)) {
                // If there exists a composite atomic-array index, our conjuncts will be split across multiple
                // SELECTs. This rewrite is to be used **solely** for the purpose of changing a DATA-SCAN into a
                // non-index-only plan branch. No nodes introduced from this rewrite will be used beyond this point.

                if (rewriteLocallyAndTransform(selectRef, context, mergedSelectRewrite, checkApplicableOnly,
                        chosenIndexes, analyzedAMs)) {
                    return true;
                }

                // If there exists a SUBPLAN in our plan, and we are conditioning on a variable, attempt to rewrite
                // this subplan to allow an array-index AM to be introduced. Again, this rewrite is to be used
                // **solely** for the purpose of changing a DATA-SCAN into a non-index-only plan branch.
                if (rewriteLocallyAndTransform(selectRef, context, selectFromSubplanRewrite, checkApplicableOnly,
                        chosenIndexes, analyzedAMs)) {
                    return true;
                }
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
                fillSubTreeIndexExprs(subTree, analyzedAMs, context, false);

                // Prune the access methods based on the function expression and access methods.
                pruneIndexCandidates(analyzedAMs, context, typeEnvironment, checkApplicableOnly);

                // Choose all indexes that will be applied.
                chooseAllIndexes(analyzedAMs, chosenIndexes);
                removeSmallerPrefixIndexes(chosenIndexes);

                if (chosenIndexes == null || chosenIndexes.isEmpty()) {
                    // We can't apply any index for this SELECT operator
                    context.addToDontApplySet(this, selectRef.getValue());
                    return false;
                }
                if (checkApplicableOnly) {
                    return true;
                }

                // Apply plan transformation using chosen index.
                boolean res;
                // Primary index applicable?
                Pair<IAccessMethod, Index> chosenPrimaryIndex = fetchPrimaryIndexAmongChosenIndexes(chosenIndexes);
                if (chosenPrimaryIndex != null) {
                    AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(chosenPrimaryIndex.first);
                    res = chosenPrimaryIndex.first.applySelectPlanTransformation(afterSelectRefs, selectRef, subTree,
                            chosenPrimaryIndex.second, analysisCtx, context);
                    context.addToDontApplySet(this, selectRef.getValue());
                } else if (chosenIndexes.size() == 1) {
                    // Index-only plan possible?
                    // Gets the analysis context for the given index.
                    AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(chosenIndexes.get(0).first);

                    // Finds the field name of each variable in the sub-tree.
                    fillFieldNamesInTheSubTree(subTree, context);

                    // Finally, try to apply plan transformation using chosen index.
                    res = chosenIndexes.get(0).first.applySelectPlanTransformation(afterSelectRefs, selectRef, subTree,
                            chosenIndexes.get(0).second, analysisCtx, context);
                    context.addToDontApplySet(this, selectRef.getValue());
                } else {
                    // Multiple secondary indexes applicable?
                    res = intersectAllSecondaryIndexes(chosenIndexes, analyzedAMs, context);
                    context.addToDontApplySet(this, selectRef.getValue());
                }

                // If the plan transformation is successful, we don't need to traverse
                // the plan any more, since if there are more SELECT operators, the next
                // trigger on this plan will find them.
                if (res) {
                    OperatorPropertiesUtil.typeOpRec(opRef, context);
                    return res;
                }
            }

            selectRef = null;
            selectOp = null;
            afterSelectRefs.add(opRef);
        }

        // Cleans the path after SELECT operator by removing the current operator in the list.
        afterSelectRefs.remove(opRef);

        return false;

    }

    @Override
    public Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods() {
        return accessMethods;
    }

    private boolean rewriteLocallyAndTransform(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            IIntroduceAccessMethodRuleLocalRewrite<SelectOperator> rewriter, boolean checkApplicableOnly,
            List<Pair<IAccessMethod, Index>> chosenIndexes, Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs)
            throws AlgebricksException {

        SelectOperator selectRewrite = rewriter.createOperator(selectOp, context);
        boolean transformationResult = false;
        if (selectRewrite != null) {
            Mutable<ILogicalOperator> selectRuleInput = new MutableObject<>(selectRewrite);
            if (checkApplicableOnly) {
                transformationResult = checkAndApplyTheSelectTransformation(selectRuleInput, context,
                        checkApplicableOnly, chosenIndexes, analyzedAMs);
            } else {
                List<Pair<IAccessMethod, Index>> chosenIndexes2 = new ArrayList<>();
                Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs2 = null;
                transformationResult = checkAndApplyTheSelectTransformation(selectRuleInput, context,
                        checkApplicableOnly, chosenIndexes2, analyzedAMs2);
            }
        }

        // Restore our state, so we can look for more optimizations if this transformation failed.
        selectOp = rewriter.restoreBeforeRewrite(null, null);
        selectRef = opRef;
        return transformationResult;
    }

    private void clear() {
        afterSelectRefs = null;
        selectRef = null;
        selectOp = null;
        selectCond = null;
        typeEnvironment = null;
        subTree.reset();
    }
}
