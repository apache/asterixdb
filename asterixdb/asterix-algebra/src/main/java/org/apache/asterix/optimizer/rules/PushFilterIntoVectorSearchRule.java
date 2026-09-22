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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
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
 * The INCLUDE columns are already declared as outputs of VECTOR_INDEX_UNNEST by the access-method phase;
 * field-access expressions in the filter are rewritten to reference those variables.
 * <p>
 * The SELECT has to sit within the search's own pipeline: the descent from it stops at a LIMIT, or at
 * any operator other than the ones the two plan shapes put between a WHERE and the search. A predicate
 * above a LIMIT filters the rows the LIMIT let through, which is a different query from filtering the
 * candidates the search ranks, so it must stay where it is.
 * <p>
 * A SELECT that does sit in the pipeline is the WHERE index selection admitted, and it was admitted because
 * the search can evaluate it. One that cannot be bound here, or a second one reaching a search that already
 * holds a condition, is therefore a plan some rule in between has reshaped into the very thing index selection
 * refused -- a filter left above a search that has already capped its candidates -- and compilation fails
 * rather than emit it. A predicate proven true and removed on the way never arrives here and needs nothing.
 */
public class PushFilterIntoVectorSearchRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
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

        UnnestMapOperator vectorUnnest = searchInfo.vectorUnnest();
        if (vectorUnnest.getSelectCondition() != null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, selectOp.getSourceLocation(),
                    "a second WHERE reached the vector search of index " + searchInfo.indexName()
                            + ", which evaluates one condition");
        }

        // One binding for both plan shapes. On the index-only plan the access-method phase already rebound
        // the predicate onto the INCLUDE columns, so there is no field access left to rewrite -- but the
        // rules that ran since may have moved part of it into an ASSIGN (ExtractCommonExpressionsRule does,
        // for a subexpression the projection shares), and only the binding's inlining puts that back. The
        // completeness guard then confirms nothing but search outputs remain, on either shape.
        VectorIncludeFilterPushdown.IndexContext idx =
                new VectorIncludeFilterPushdown.IndexContext(searchInfo.includeFieldNames(), searchInfo.recordType(),
                        searchInfo.isQuantized(), searchInfo.numPrimaryKeys(), searchInfo.recordVars());
        ILogicalExpression bound = VectorIncludeFilterPushdown.bindPredicate(selectOp.getCondition().getValue(),
                selectOp, idx, context, VectorIncludeFilterPushdown.getIncludeColumns(vectorUnnest));
        if (bound == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, selectOp.getSourceLocation(),
                    "the WHERE above the vector search of index " + searchInfo.indexName()
                            + " reads what the search cannot evaluate, though index selection admits only a WHERE it can");
        }
        VectorIncludeFilterPushdown.apply(vectorUnnest, bound);
        return dropSelect(opRef, selectOp, vectorUnnest, op, context);
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
     *                   (and PK) variables of THIS search's primary-index lookup, excluding its meta
     *                   record. A field access must be rooted at one of these to be a candidate for
     *                   pushdown.
     * @param indexName  the searched index, for diagnostics
     */
    private record VectorSearchInfo(UnnestMapOperator vectorUnnest, List<List<String>> includeFieldNames,
            ARecordType recordType, boolean isQuantized, int numPrimaryKeys, Set<LogicalVariable> recordVars,
            String indexName) {
    }

    /**
     * Finds the vector index search this SELECT's predicate can be moved into, or {@code null} if there is
     * none within the search's own pipeline.
     */
    private VectorSearchInfo findVectorIndexUnnest(SelectOperator selectOp, IOptimizationContext context)
            throws AlgebricksException {
        return searchForVectorUnnest(selectOp.getInputs().get(0).getValue(), context, new ArrayList<>());
    }

    /**
     * Walks down from the SELECT to a VECTOR_INDEX_UNNEST, collecting on the way the index-search
     * unnest-maps it passes through — the primary-index lookup that materializes the dataset record the
     * filter reads. Their variables become the pushdown bases in {@link #buildSearchInfo}, which is where the
     * dataset is known and the meta record can be told apart from the record.
     * <p>
     * Only the operators the two plan shapes put between a WHERE and the search are walked through, and each
     * must have a single input. A LIMIT in particular ends the walk: a predicate above it applies to the rows
     * the LIMIT let through, and moving it into the search would instead choose which candidates the search
     * ranks — "the k nearest that pass" in place of "those of the k nearest that pass". A join ends it too, so
     * a predicate over another branch's record can never be resolved against this index's INCLUDE columns.
     */
    private VectorSearchInfo searchForVectorUnnest(ILogicalOperator op, IOptimizationContext context,
            List<Pair<UnnestMapOperator, AccessMethodJobGenParams>> recordSources) throws AlgebricksException {
        switch (op.getOperatorTag()) {
            case UNNEST_MAP: {
                UnnestMapOperator unnest = (UnnestMapOperator) op;
                AccessMethodJobGenParams params = indexSearchParams(unnest);
                if (params == null) {
                    return null;
                }
                if (params.getIndexType() == IndexType.VTREE) {
                    return buildSearchInfo(unnest, params, context, recordSources);
                }
                recordSources.add(new Pair<>(unnest, params));
                break;
            }
            case ASSIGN:
            case SELECT:
            case PROJECT:
            case ORDER:
            case DISTINCT:
            case EXCHANGE:
                break;
            default:
                return null;
        }
        if (op.getInputs().size() != 1) {
            return null;
        }
        return searchForVectorUnnest(op.getInputs().get(0).getValue(), context, recordSources);
    }

    /** The parameters of the index search {@code unnest} performs, or {@code null} if it is not one. */
    private static AccessMethodJobGenParams indexSearchParams(UnnestMapOperator unnest) throws AlgebricksException {
        ILogicalExpression expr = unnest.getExpressionRef().getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        if (!funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INDEX_SEARCH)) {
            return null;
        }
        AccessMethodJobGenParams params = new AccessMethodJobGenParams();
        params.readFromFuncArgs(funcExpr.getArguments());
        return params;
    }

    /**
     * Builds VectorSearchInfo from the found vector index.
     */
    private VectorSearchInfo buildSearchInfo(UnnestMapOperator unnest, AccessMethodJobGenParams params,
            IOptimizationContext context, List<Pair<UnnestMapOperator, AccessMethodJobGenParams>> recordSources)
            throws AlgebricksException {

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

        // Only this dataset's own primary-index lookup materializes the record the INCLUDE paths are resolved
        // against; a field access on any other lookup's record must be left alone so the completeness guard
        // declines it. On a collection with a meta part the lookup produces [pk..., record, meta]. Drop the
        // meta variable: resolveFieldPath resolves a field access against the dataset's record type, so
        // admitting it would let `WHERE meta(m).year > 0` bind to the record's INCLUDE column of the same
        // name and filter on the wrong value.
        Set<LogicalVariable> recordVars = new HashSet<>();
        for (Pair<UnnestMapOperator, AccessMethodJobGenParams> lookup : recordSources) {
            if (!isPrimaryLookupOf(lookup.second, dataset)) {
                continue;
            }
            List<LogicalVariable> vars = lookup.first.getVariables();
            recordVars.addAll(vars.subList(0, dataset.hasMetaPart() ? vars.size() - 1 : vars.size()));
        }

        return new VectorSearchInfo(unnest, details.getIncludeFieldNames(), recordType,
                details.getVectorParameters().isQuantized(), dataset.getPrimaryKeys().size(), recordVars,
                index.getIndexName());
    }

    /** Whether {@code params} describe a search of {@code dataset}'s primary index, which is named after it. */
    private static boolean isPrimaryLookupOf(AccessMethodJobGenParams params, Dataset dataset) {
        return Objects.equals(params.getDatabaseName(), dataset.getDatabaseName())
                && Objects.equals(params.getDataverseName(), dataset.getDataverseName())
                && Objects.equals(params.getDatasetName(), dataset.getDatasetName())
                && Objects.equals(params.getIndexName(), dataset.getDatasetName());
    }
}
