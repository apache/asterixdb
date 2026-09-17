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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Pushes filter conditions into a vector index search when the filter only references
 * INCLUDE fields of that index. Must run in physicalRewritesTopLevel (after
 * SetClosedRecordConstructorsRule) so that record constructors have already been closed.
 *
 * Pattern:
 * <pre>
 *   SELECT (condition on INCLUDE fields)
 *     └── ASSIGN* (optional)
 *           └── PRIMARY_INDEX_UNNEST
 *                 └── ...
 *                       └── VECTOR_INDEX_UNNEST [$pk]
 * </pre>
 *
 * Transforms to:
 * <pre>
 *   ASSIGN* (optional, SELECT removed)
 *     └── PRIMARY_INDEX_UNNEST
 *           └── ...
 *                 └── VECTOR_INDEX_UNNEST [$pk, $includeField1, ...]
 *                       selectCondition: (rewritten to use $includeField1, ...)
 * </pre>
 *
 * New variables are created for the INCLUDE fields produced by VECTOR_INDEX_UNNEST, and
 * field-access expressions in the filter are rewritten to reference those variables.
 */
public class PushFilterIntoVectorSearchRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();

        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        SelectOperator selectOp = (SelectOperator) op;

        VectorSearchInfo searchInfo = findVectorIndexUnnest(selectOp, context);
        if (searchInfo == null) {
            return false;
        }

        // Index-only shape: the access-method phase already declared the INCLUDE columns on the unnest-map
        // and rebound the predicate to them, leaving it here as an ordinary SELECT so that the rules in
        // between saw an ordinary plan. Nothing is left to resolve -- just move it into the select
        // condition, which is the one place the runtime reads it from.
        if (VectorIncludeFilterPushdown.hasDeclaredFilterVariables(searchInfo.vectorUnnest())) {
            Set<LogicalVariable> conditionVars = new HashSet<>();
            selectOp.getCondition().getValue().getUsedVariables(conditionVars);
            if (!searchInfo.vectorUnnest().getVariables().containsAll(conditionVars)) {
                return false;
            }
            searchInfo.vectorUnnest().setSelectCondition(new MutableObject<>(selectOp.getCondition().getValue()));
            return dropSelect(opRef, selectOp, searchInfo.vectorUnnest(), op, context);
        }

        // The whole decision — which field paths the predicate reads, whether the index's INCLUDE list
        // covers them, and what the rewritten predicate looks like — lives in VectorIncludeFilterPushdown,
        // shared with the index-only gate in IntroduceTopKAccessMethodRule. See that class for why the two
        // must not be separate implementations.
        VectorIncludeFilterPushdown.IndexContext idx =
                new VectorIncludeFilterPushdown.IndexContext(searchInfo.includeFieldNames(), searchInfo.recordType(),
                        searchInfo.isQuantized(), searchInfo.numPrimaryKeys(), searchInfo.recordVars());
        VectorIncludeFilterPushdown.PushedIncludeFilter pushed = VectorIncludeFilterPushdown
                .analyze(selectOp.getCondition().getValue(), selectOp, idx, context, context::newVar);
        if (pushed == null) {
            return false;
        }

        VectorIncludeFilterPushdown.apply(searchInfo.vectorUnnest(), pushed);
        return dropSelect(opRef, selectOp, searchInfo.vectorUnnest(), op, context);
    }

    /** Remove the SELECT whose predicate now lives in the vector search, and retype what changed. */
    private boolean dropSelect(Mutable<ILogicalOperator> opRef, SelectOperator selectOp, UnnestMapOperator vectorUnnest,
            ILogicalOperator visitedOp, IOptimizationContext context) throws AlgebricksException {
        opRef.setValue(selectOp.getInputs().get(0).getValue());
        context.addToDontApplySet(this, visitedOp);
        context.computeAndSetTypeEnvironmentForOperator(vectorUnnest);
        OperatorPropertiesUtil.typeOpRec(opRef, context);
        return true;
    }

    /**
     * Information about a vector index search found in the plan.
     *
     * @param recordVars the variables produced between the SELECT and the vector search — i.e. the record
     *                   (and PK) variables of THIS search's primary-index lookup. A field access must be
     *                   rooted at one of these to be a candidate for pushdown.
     */
    private record VectorSearchInfo(UnnestMapOperator vectorUnnest, List<List<String>> includeFieldNames,
            ARecordType recordType, boolean isQuantized, int numPrimaryKeys, Set<LogicalVariable> recordVars) {
    }

    /**
     * Finds VECTOR_INDEX_UNNEST below the SELECT operator, skipping intervening ASSIGNs.
     */
    private VectorSearchInfo findVectorIndexUnnest(SelectOperator selectOp, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator current = selectOp.getInputs().get(0).getValue();
        while (current.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            current = current.getInputs().get(0).getValue();
        }
        return searchForVectorUnnest(current, context, new HashSet<>());
    }

    /**
     * Recursively searches for a VECTOR_INDEX_UNNEST under the given operator, accumulating on the way down
     * the variables produced by the non-vector unnest-maps it passes through — the primary-index lookup that
     * materializes the dataset record the filter reads.
     */
    private VectorSearchInfo searchForVectorUnnest(ILogicalOperator op, IOptimizationContext context,
            Set<LogicalVariable> recordVars) throws AlgebricksException {

        if (op.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            UnnestMapOperator unnest = (UnnestMapOperator) op;
            ILogicalExpression expr = unnest.getExpressionRef().getValue();

            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

                if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INDEX_SEARCH)) {
                    AccessMethodJobGenParams params = new AccessMethodJobGenParams();
                    params.readFromFuncArgs(funcExpr.getArguments());

                    if (params.getIndexType() == IndexType.VTREE) {
                        if (unnest.getSelectCondition() != null) {
                            return null;
                        }
                        return buildSearchInfo(unnest, params, context, recordVars);
                    }
                }
            }
            recordVars.addAll(unnest.getVariables());
        }

        for (Mutable<ILogicalOperator> inputRef : op.getInputs()) {
            VectorSearchInfo result = searchForVectorUnnest(inputRef.getValue(), context, recordVars);
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    /**
     * Builds VectorSearchInfo from the found vector index.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    private VectorSearchInfo buildSearchInfo(UnnestMapOperator unnest, AccessMethodJobGenParams params,
            IOptimizationContext context, Set<LogicalVariable> recordVars) throws AlgebricksException {

        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();

        Dataset dataset = mp.findDataset(params.getDatabaseName(), params.getDataverseName(), params.getDatasetName());
        if (dataset == null) {
            return null;
        }

        Index index = mp.getIndex(params.getDatabaseName(), params.getDataverseName(), params.getDatasetName(),
                params.getIndexName());
        if (index == null || index.getIndexType() != IndexType.VTREE) {
            return null;
        }

        Index.VectorIndexDetails details = (Index.VectorIndexDetails) index.getIndexDetails();

        ARecordType recordType = (ARecordType) mp.findType(dataset.getItemTypeDatabaseName(),
                dataset.getItemTypeDataverseName(), dataset.getItemTypeName());

        return new VectorSearchInfo(unnest, details.getIncludeFieldNames(), recordType,
                details.getVectorParameters().isQuantized(), dataset.getPrimaryKeys().size(), recordVars);
    }
}
