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
import java.util.List;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.ArrayIndexUtil;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;

// TODO (GLENN): Refactor the BTreeAccessMethod class and this class to extend a new "AbstractBTreeAccessMethod" class.
/**
 * Class for helping rewrite rules to choose and apply array BTree indexes.
 */
public class ArrayBTreeAccessMethod extends BTreeAccessMethod {
    public static final ArrayBTreeAccessMethod INSTANCE = new ArrayBTreeAccessMethod();

    @Override
    public boolean matchAllIndexExprs(Index index) {
        // Similar to BTree "matchAllIndexExprs", we only require all expressions to be matched if this is a composite
        // key index with an unknowable field.
        return ((Index.ArrayIndexDetails) index.getIndexDetails()).getElementList().stream()
                .map(e -> e.getProjectList().size()).reduce(0, Integer::sum) > 1 && hasUnknownableField(index);
    }

    @Override
    public boolean matchPrefixIndexExprs(Index index) {
        return !matchAllIndexExprs(index);
    }

    private boolean hasUnknownableField(Index index) {
        if (index.isSecondaryIndex() && index.getIndexDetails().isOverridingKeyFieldTypes() && !index.isEnforced()) {
            return true;
        }
        for (Index.ArrayIndexElement e : ((Index.ArrayIndexDetails) index.getIndexDetails()).getElementList()) {
            for (int i = 0; i < e.getProjectList().size(); i++) {
                if (NonTaggedFormatUtil.isOptional(e.getTypeList().get(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean applyJoinPlanTransformation(List<Mutable<ILogicalOperator>> afterJoinRefs,
            Mutable<ILogicalOperator> joinRef, OptimizableOperatorSubTree leftSubTree,
            OptimizableOperatorSubTree rightSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, boolean isLeftOuterJoin, boolean isLeftOuterJoinWithSpecialGroupBy)
            throws AlgebricksException {
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) joinRef.getValue();
        Mutable<ILogicalExpression> conditionRef = joinOp.getCondition();
        Dataset dataset = analysisCtx.getIndexDatasetMap().get(chosenIndex);
        OptimizableOperatorSubTree indexSubTree, probeSubTree;

        // We assume that the left subtree is the outer branch and the right subtree is the inner branch. This
        // assumption holds true since we only use an index from the right subtree. The following is just a sanity
        // check.
        if (rightSubTree.hasDataSourceScan()
                && dataset.getDatasetName().equals(rightSubTree.getDataset().getDatasetName())) {
            indexSubTree = rightSubTree;
            probeSubTree = leftSubTree;
        } else {
            return false;
        }

        LogicalVariable newNullPlaceHolderVar = null;
        if (isLeftOuterJoin) {
            // Gets a new null place holder variable that is the first field variable of the primary key from the
            // indexSubTree's datasourceScanOp. We need this for all left outer joins, even those that do not have
            // a special GroupBy.
            newNullPlaceHolderVar = indexSubTree.getDataSourceVariables().get(0);

            // For all INNER-UNNESTs associated with the inner subtree (i.e. the index subtree) to extract the
            // secondary keys, transform these UNNESTs to LEFT-OUTER-UNNESTs. This is to ensure that probe entries w/o
            // a corresponding secondary key entry are not incorrectly removed. This will not invalidate our fetched
            // entries because *all* index entries have a non-empty array.
            ILogicalOperator workingOp = indexSubTree.getRoot(), rootOp = indexSubTree.getRoot(), previousOp = null;
            while (!workingOp.getInputs().isEmpty()) {
                if (workingOp.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                    UnnestOperator oldUnnest = (UnnestOperator) workingOp;
                    LeftOuterUnnestOperator newUnnest = new LeftOuterUnnestOperator(oldUnnest.getVariable(),
                            new MutableObject<>(oldUnnest.getExpressionRef().getValue()));
                    newUnnest.setSourceLocation(oldUnnest.getSourceLocation());
                    newUnnest.getInputs().addAll(oldUnnest.getInputs());
                    newUnnest.setExecutionMode(oldUnnest.getExecutionMode());
                    context.computeAndSetTypeEnvironmentForOperator(newUnnest);
                    if (workingOp.equals(rootOp)) {
                        rootOp = newUnnest;
                        workingOp = newUnnest;
                    } else if (previousOp != null) {
                        previousOp.getInputs().clear();
                        previousOp.getInputs().add(new MutableObject<>(newUnnest));
                        context.computeAndSetTypeEnvironmentForOperator(previousOp);
                    }
                }
                previousOp = workingOp;
                workingOp = workingOp.getInputs().get(0).getValue();
            }
            indexSubTree.setRoot(rootOp);
            indexSubTree.setRootRef(new MutableObject<>(rootOp));
            joinOp.getInputs().remove(1);
            joinOp.getInputs().add(1, new MutableObject<>(rootOp));
            context.computeAndSetTypeEnvironmentForOperator(joinOp);
        }

        ILogicalOperator indexSearchOp = createIndexSearchPlan(afterJoinRefs, joinRef, conditionRef,
                indexSubTree.getAssignsAndUnnestsRefs(), indexSubTree, probeSubTree, chosenIndex, analysisCtx, true,
                isLeftOuterJoin, true, context, newNullPlaceHolderVar);
        if (indexSearchOp == null) {
            return false;
        }

        return AccessMethodUtils.finalizeJoinPlanTransformation(afterJoinRefs, joinRef, indexSubTree, probeSubTree,
                analysisCtx, context, isLeftOuterJoin, isLeftOuterJoinWithSpecialGroupBy, indexSearchOp,
                newNullPlaceHolderVar, conditionRef, dataset);
    }

    @Override
    public ILogicalOperator createIndexSearchPlan(List<Mutable<ILogicalOperator>> afterTopOpRefs,
            Mutable<ILogicalOperator> topOpRef, Mutable<ILogicalExpression> conditionRef,
            List<Mutable<ILogicalOperator>> assignBeforeTheOpRefs, OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            boolean retainInput, boolean retainMissing, boolean requiresBroadcast, IOptimizationContext context,
            LogicalVariable newMissingPlaceHolderForLOJ) throws AlgebricksException {

        Index.ArrayIndexDetails chosenIndexDetails = (Index.ArrayIndexDetails) chosenIndex.getIndexDetails();
        List<List<String>> chosenIndexKeyFieldNames = new ArrayList<>();
        List<IAType> chosenIndexKeyFieldTypes = new ArrayList<>();
        List<Integer> chosenIndexKeyFieldSourceIndicators = new ArrayList<>();
        for (Index.ArrayIndexElement e : chosenIndexDetails.getElementList()) {
            for (int i = 0; i < e.getProjectList().size(); i++) {
                chosenIndexKeyFieldNames
                        .add(ArrayIndexUtil.getFlattenedKeyFieldNames(e.getUnnestList(), e.getProjectList().get(i)));
                chosenIndexKeyFieldTypes.add(e.getTypeList().get(i));
                chosenIndexKeyFieldSourceIndicators.add(e.getSourceIndicator());
            }
        }

        return createBTreeIndexSearchPlan(afterTopOpRefs, topOpRef, conditionRef, assignBeforeTheOpRefs, indexSubTree,
                probeSubTree, chosenIndex, analysisCtx, retainInput, retainMissing, requiresBroadcast, context,
                newMissingPlaceHolderForLOJ, chosenIndexKeyFieldNames, chosenIndexKeyFieldTypes,
                chosenIndexKeyFieldSourceIndicators);
    }

    @Override
    protected IAType getIndexedKeyType(Index.IIndexDetails chosenIndexDetails, int keyPos) throws CompilationException {
        Index.ArrayIndexDetails arrayIndexDetails = (Index.ArrayIndexDetails) chosenIndexDetails;
        int elementPos = 0;
        for (Index.ArrayIndexElement e : arrayIndexDetails.getElementList()) {
            for (int i = 0; i < e.getProjectList().size(); i++) {
                if (elementPos == keyPos) {
                    return e.getTypeList().get(i);
                }
                elementPos++;
            }
        }

        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                "No array index element found, but using " + "an array access method.");
    }

    @Override
    public boolean matchIndexType(IndexType indexType) {
        return indexType == IndexType.ARRAY;
    }

    @Override
    public String getName() {
        return "ARRAY_BTREE_ACCESS_METHOD";
    }

    @Override
    public int compareTo(IAccessMethod o) {
        return this.getName().compareTo(o.getName());
    }

}
