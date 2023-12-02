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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.algebra.operators.CommitOperator;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceIndex;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.utils.ArrayIndexUtil;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator.Kind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * This rule matches the pattern:
 * assign --> insert-delete-upsert --> sink
 * and produces
 * assign --> insert-delete-upsert --> *(secondary indexes index-insert-delete-upsert) --> sink
 */
public class IntroduceSecondaryIndexInsertDeleteRule implements IAlgebraicRewriteRule {
    private IOptimizationContext context;
    private SourceLocation sourceLoc;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getValue();
        if (op0.getOperatorTag() != LogicalOperatorTag.DELEGATE_OPERATOR
                && op0.getOperatorTag() != LogicalOperatorTag.SINK) {
            return false;
        }
        if (op0.getOperatorTag() == LogicalOperatorTag.DELEGATE_OPERATOR) {
            DelegateOperator eOp = (DelegateOperator) op0;
            if (!(eOp.getDelegate() instanceof CommitOperator)) {
                return false;
            }
        }
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) op0.getInputs().get(0).getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE_UPSERT) {
            return false;
        }
        /** find the record variable */
        InsertDeleteUpsertOperator primaryIndexModificationOp =
                (InsertDeleteUpsertOperator) op0.getInputs().get(0).getValue();
        boolean isBulkload = primaryIndexModificationOp.isBulkload();
        ILogicalExpression newRecordExpr = primaryIndexModificationOp.getPayloadExpression().getValue();
        List<Mutable<ILogicalExpression>> newMetaExprs =
                primaryIndexModificationOp.getAdditionalNonFilteringExpressions();
        LogicalVariable newRecordVar;
        LogicalVariable newMetaVar = null;
        sourceLoc = primaryIndexModificationOp.getSourceLocation();
        this.context = context;

        /**
         * inputOp is the assign operator which extracts primary keys from the input
         * variables (record or meta)
         */
        AbstractLogicalOperator inputOp =
                (AbstractLogicalOperator) primaryIndexModificationOp.getInputs().get(0).getValue();
        newRecordVar = getRecordVar(inputOp, newRecordExpr, 0);
        if (newMetaExprs != null && !newMetaExprs.isEmpty()) {
            if (newMetaExprs.size() > 1) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Number of meta records can't be more than 1. Number of meta records found = "
                                + newMetaExprs.size());
            }
            newMetaVar = getRecordVar(inputOp, newMetaExprs.get(0).getValue(), 1);
        }

        /*
         * At this point, we have the record variable and the insert/delete/upsert operator
         * Note: We have two operators:
         * 1. An InsertDeleteOperator (primary)
         * 2. An IndexInsertDeleteOperator (secondary)
         * The current primaryIndexModificationOp is of the first type
         */

        DataSource datasetSource = (DataSource) primaryIndexModificationOp.getDataSource();
        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
        DataverseName dataverseName = datasetSource.getId().getDataverseName();
        String database = datasetSource.getId().getDatabaseName();
        String datasetName = datasetSource.getId().getDatasourceName();
        Dataset dataset = mp.findDataset(database, dataverseName, datasetName);
        if (dataset == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                    MetadataUtil.dataverseName(database, dataverseName, mp.isUsingDatabase()));
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return false;
        }

        // Create operators for secondary index insert / delete.
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType =
                mp.findType(dataset.getItemTypeDatabaseName(), dataset.getItemTypeDataverseName(), itemTypeName);
        if (itemType.getTypeTag() != ATypeTag.OBJECT) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Only record types can be indexed.");
        }
        ARecordType recType = (ARecordType) itemType;
        // meta type
        ARecordType metaType = null;
        if (dataset.hasMetaPart()) {
            metaType = (ARecordType) mp.findType(dataset.getMetaItemTypeDatabaseName(),
                    dataset.getMetaItemTypeDataverseName(), dataset.getMetaItemTypeName());
        }
        recType = (ARecordType) mp.findTypeForDatasetWithoutType(recType, metaType, dataset);

        List<Index> indexes =
                mp.getDatasetIndexes(dataset.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName());
        Stream<Index> indexStream = indexes.stream();
        indexStream = indexStream.filter(index -> index.getIndexType() != IndexType.SAMPLE);
        if (primaryIndexModificationOp.getOperation() == Kind.INSERT && !primaryIndexModificationOp.isBulkload()) {
            // for insert, primary key index is handled together when primary index
            indexStream = indexStream.filter(index -> !index.isPrimaryKeyIndex());
        }
        indexes = indexStream.collect(Collectors.toList());

        // Put an n-gram or a keyword index in the later stage of index-update,
        // since TokenizeOperator needs to be involved.
        Collections.sort(indexes, (o1, o2) -> o1.getIndexType().ordinal() - o2.getIndexType().ordinal());

        // Set the top operator pointer to the primary IndexInsertDeleteOperator
        ILogicalOperator currentTop = primaryIndexModificationOp;

        // At this point, we have the data type info, and the indexes info as well
        int secondaryIndexTotalCnt = indexes.size() - 1;
        if (secondaryIndexTotalCnt > 0) {
            op0.getInputs().clear();
        } else {
            return false;
        }
        // Initialize inputs to the SINK operator Op0 (The SINK) is now without input
        // Prepare filtering field information (This is the filter created using the "filter with" key word in the
        // create dataset ddl)
        List<String> filteringFields = ((InternalDatasetDetails) dataset.getDatasetDetails()).getFilterField();
        List<LogicalVariable> filteringVars;
        List<Mutable<ILogicalExpression>> filteringExpressions = null;

        if (filteringFields != null) {
            // The filter field var already exists. we can simply get it from the insert op
            filteringVars = new ArrayList<>();
            filteringExpressions = new ArrayList<>();
            for (Mutable<ILogicalExpression> filteringExpression : primaryIndexModificationOp
                    .getAdditionalFilteringExpressions()) {
                filteringExpression.getValue().getUsedVariables(filteringVars);
                for (LogicalVariable var : filteringVars) {
                    VariableReferenceExpression varRef = new VariableReferenceExpression(var);
                    varRef.setSourceLocation(filteringExpression.getValue().getSourceLocation());
                    filteringExpressions.add(new MutableObject<ILogicalExpression>(varRef));
                }
            }
        }

        // Replicate Operator is applied only when doing the bulk-load.
        ReplicateOperator replicateOp = null;
        if (secondaryIndexTotalCnt > 1 && isBulkload) {
            // Split the logical plan into "each secondary index update branch"
            // to replicate each <PK,OBJECT> pair.
            replicateOp = new ReplicateOperator(secondaryIndexTotalCnt);
            replicateOp.setSourceLocation(sourceLoc);
            replicateOp.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
            replicateOp.setExecutionMode(ExecutionMode.PARTITIONED);
            context.computeAndSetTypeEnvironmentForOperator(replicateOp);
            currentTop = replicateOp;
        }

        /*
         * The two maps are used to store variables to which [casted] field access is assigned.
         * One for the beforeOp record and the other for the new record.
         * There are two uses for these maps:
         * 1. used for shared fields in indexes with overlapping keys.
         * 2. used for setting variables of secondary keys for each secondary index operator.
         */
        Map<IndexFieldId, LogicalVariable> fieldVarsForBeforeOperation = new HashMap<>();
        Map<IndexFieldId, LogicalVariable> fieldVarsForNewRecord = new HashMap<>();
        /*
         * if the index is enforcing field types (For open indexes), We add a cast
         * operator to ensure type safety
         */
        if (primaryIndexModificationOp.getOperation() == Kind.INSERT
                || primaryIndexModificationOp.getOperation() == Kind.UPSERT
                /* Actually, delete should not be here but it is now until issue
                 * https://issues.apache.org/jira/browse/ASTERIXDB-1507
                 * is solved
                 */
                || primaryIndexModificationOp.getOperation() == Kind.DELETE) {
            injectFieldAccessesForIndexes(dataset, indexes, fieldVarsForNewRecord, recType, metaType, newRecordVar,
                    newMetaVar, primaryIndexModificationOp, false);
            if (replicateOp != null) {
                context.computeAndSetTypeEnvironmentForOperator(replicateOp);
            }
        }
        if (primaryIndexModificationOp.getOperation() == Kind.UPSERT
        /* Actually, delete should be here but it is not until issue
         * https://issues.apache.org/jira/browse/ASTERIXDB-1507
         * is solved
         */) {
            List<LogicalVariable> beforeOpMetaVars = primaryIndexModificationOp.getBeforeOpAdditionalNonFilteringVars();
            LogicalVariable beforeOpMetaVar = beforeOpMetaVars == null ? null : beforeOpMetaVars.get(0);
            currentTop = injectFieldAccessesForIndexes(dataset, indexes, fieldVarsForBeforeOperation, recType, metaType,
                    primaryIndexModificationOp.getBeforeOpRecordVar(), beforeOpMetaVar, currentTop, true);
        }

        // Add the appropriate SIDX maintenance operations.
        for (Index index : indexes) {
            if (!index.isSecondaryIndex()) {
                continue;
            }

            // Get the secondary fields names and types
            List<List<String>> secondaryKeyFields = null;
            List<IAType> secondaryKeyTypes = null;
            List<Integer> secondaryKeySources = null;
            switch (Index.IndexCategory.of(index.getIndexType())) {
                case VALUE:
                    Index.ValueIndexDetails valueIndexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
                    secondaryKeyFields = valueIndexDetails.getKeyFieldNames();
                    secondaryKeyTypes = valueIndexDetails.getKeyFieldTypes();
                    secondaryKeySources = valueIndexDetails.getKeyFieldSourceIndicators();
                    break;
                case TEXT:
                    Index.TextIndexDetails textIndexDetails = (Index.TextIndexDetails) index.getIndexDetails();
                    secondaryKeyFields = textIndexDetails.getKeyFieldNames();
                    secondaryKeyTypes = textIndexDetails.getKeyFieldTypes();
                    secondaryKeySources = textIndexDetails.getKeyFieldSourceIndicators();
                    break;
                case ARRAY:
                    // These details are handled separately for array indexes.
                    break;
                default:
                    continue;
            }

            // Set our key variables and expressions for non-array indexes. Our secondary keys for array indexes will
            // always be an empty list.
            List<LogicalVariable> secondaryKeyVars = new ArrayList<>();
            List<LogicalVariable> beforeOpSecondaryKeyVars = new ArrayList<>();
            List<Mutable<ILogicalExpression>> secondaryExpressions = new ArrayList<>();
            List<Mutable<ILogicalExpression>> beforeOpSecondaryExpressions = new ArrayList<>();
            ILogicalOperator replicateOutput;
            if (!index.getIndexType().equals(IndexType.ARRAY)) {
                for (int i = 0; i < secondaryKeyFields.size(); i++) {
                    IAType skType = secondaryKeyTypes.get(i);
                    Integer skSrc = secondaryKeySources.get(i);
                    List<String> skName = secondaryKeyFields.get(i);
                    ARecordType sourceType = dataset.hasMetaPart()
                            ? skSrc.intValue() == Index.RECORD_INDICATOR ? recType : metaType : recType;
                    IndexFieldId indexFieldId = createIndexFieldId(index, skName, skType, skSrc, sourceType, sourceLoc);
                    LogicalVariable skVar = fieldVarsForNewRecord.get(indexFieldId);
                    secondaryKeyVars.add(skVar);
                    VariableReferenceExpression skVarRef = new VariableReferenceExpression(skVar);
                    skVarRef.setSourceLocation(sourceLoc);
                    secondaryExpressions.add(new MutableObject<>(skVarRef));
                    if (primaryIndexModificationOp.getOperation() == Kind.UPSERT) {
                        LogicalVariable beforeKeyVar = fieldVarsForBeforeOperation.get(indexFieldId);
                        beforeOpSecondaryKeyVars.add(beforeKeyVar);
                        VariableReferenceExpression varRef = new VariableReferenceExpression(beforeKeyVar);
                        varRef.setSourceLocation(sourceLoc);
                        beforeOpSecondaryExpressions.add(new MutableObject<>(varRef));
                    }
                }
            }

            IndexInsertDeleteUpsertOperator indexUpdate;
            if (index.getIndexType() != IndexType.RTREE) {
                // B-Tree, inverted index, array index
                // Create an expression per key
                Mutable<ILogicalExpression> filterExpression =
                        createFilterExpression(index, secondaryKeyVars, context.getOutputTypeEnvironment(currentTop),
                                index.getIndexDetails().isOverridingKeyFieldTypes());
                Mutable<ILogicalExpression> beforeOpFilterExpression = null;
                if (primaryIndexModificationOp.getOperation() == Kind.UPSERT) {
                    beforeOpFilterExpression = createFilterExpression(index, beforeOpSecondaryKeyVars,
                            context.getOutputTypeEnvironment(currentTop),
                            index.getIndexDetails().isOverridingKeyFieldTypes());
                }
                DataSourceIndex dataSourceIndex = new DataSourceIndex(index, database, dataverseName, datasetName, mp);

                // Introduce the TokenizeOperator only when doing bulk-load,
                // and index type is keyword or n-gram.
                if (index.getIndexType() != IndexType.BTREE && index.getIndexType() != IndexType.ARRAY
                        && primaryIndexModificationOp.isBulkload()) {
                    // Note: Bulk load case, we don't need to take care of it for upsert operation
                    // Check whether the index is length-partitioned or not.
                    // If partitioned, [input variables to TokenizeOperator,
                    // token, number of token] pairs will be generated and
                    // fed into the IndexInsertDeleteOperator.
                    // If not, [input variables, token] pairs will be generated
                    // and fed into the IndexInsertDeleteOperator.
                    // Input variables are passed since TokenizeOperator is not an
                    // filtering operator.
                    boolean isPartitioned = index.getIndexType() == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                            || index.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX;

                    // Create a new logical variable - token
                    List<LogicalVariable> tokenizeKeyVars = new ArrayList<>();
                    List<Mutable<ILogicalExpression>> tokenizeKeyExprs = new ArrayList<>();
                    LogicalVariable tokenVar = context.newVar();
                    tokenizeKeyVars.add(tokenVar);
                    VariableReferenceExpression tokenVarRef = new VariableReferenceExpression(tokenVar);
                    tokenVarRef.setSourceLocation(sourceLoc);
                    tokenizeKeyExprs.add(new MutableObject<ILogicalExpression>(tokenVarRef));

                    // Check the field type of the secondary key.
                    IAType secondaryKeyType;
                    Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(index,
                            secondaryKeyTypes.get(0), secondaryKeyFields.get(0), recType);
                    secondaryKeyType = keyPairType.first;

                    List<Object> varTypes = new ArrayList<>();
                    varTypes.add(NonTaggedFormatUtil.getTokenType(secondaryKeyType));

                    // If the index is a length-partitioned, then create
                    // additional variable - number of token.
                    // We use a special type for the length-partitioned index.
                    // The type is short, and this does not contain type info.
                    if (isPartitioned) {
                        LogicalVariable lengthVar = context.newVar();
                        tokenizeKeyVars.add(lengthVar);
                        VariableReferenceExpression lengthVarRef = new VariableReferenceExpression(lengthVar);
                        lengthVarRef.setSourceLocation(sourceLoc);
                        tokenizeKeyExprs.add(new MutableObject<ILogicalExpression>(lengthVarRef));
                        varTypes.add(BuiltinType.SHORTWITHOUTTYPEINFO);
                    }

                    // TokenizeOperator to tokenize [SK, PK] pairs
                    TokenizeOperator tokenUpdate = new TokenizeOperator(dataSourceIndex,
                            OperatorManipulationUtil
                                    .cloneExpressions(primaryIndexModificationOp.getPrimaryKeyExpressions()),
                            secondaryExpressions, tokenizeKeyVars,
                            filterExpression != null
                                    ? new MutableObject<>(filterExpression.getValue().cloneExpression()) : null,
                            primaryIndexModificationOp.getOperation(), primaryIndexModificationOp.isBulkload(),
                            isPartitioned, varTypes);
                    tokenUpdate.setSourceLocation(sourceLoc);
                    tokenUpdate.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
                    context.computeAndSetTypeEnvironmentForOperator(tokenUpdate);
                    replicateOutput = tokenUpdate;
                    indexUpdate = new IndexInsertDeleteUpsertOperator(dataSourceIndex,
                            OperatorManipulationUtil
                                    .cloneExpressions(primaryIndexModificationOp.getPrimaryKeyExpressions()),
                            tokenizeKeyExprs, filterExpression, beforeOpFilterExpression,
                            primaryIndexModificationOp.getOperation(), primaryIndexModificationOp.isBulkload(),
                            primaryIndexModificationOp.getAdditionalNonFilteringExpressions() == null ? 0
                                    : primaryIndexModificationOp.getAdditionalNonFilteringExpressions().size());
                    indexUpdate.setSourceLocation(sourceLoc);
                    indexUpdate.setAdditionalFilteringExpressions(
                            OperatorManipulationUtil.cloneExpressions(filteringExpressions));
                    indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(tokenUpdate));
                } else {
                    // When TokenizeOperator is not needed
                    indexUpdate = new IndexInsertDeleteUpsertOperator(dataSourceIndex,
                            OperatorManipulationUtil
                                    .cloneExpressions(primaryIndexModificationOp.getPrimaryKeyExpressions()),
                            secondaryExpressions, filterExpression, beforeOpFilterExpression,
                            primaryIndexModificationOp.getOperation(), primaryIndexModificationOp.isBulkload(),
                            primaryIndexModificationOp.getAdditionalNonFilteringExpressions() == null ? 0
                                    : primaryIndexModificationOp.getAdditionalNonFilteringExpressions().size());
                    indexUpdate.setSourceLocation(sourceLoc);
                    indexUpdate.setAdditionalFilteringExpressions(
                            OperatorManipulationUtil.cloneExpressions(filteringExpressions));
                    replicateOutput = indexUpdate;
                    // We add the necessary expressions for upsert
                    if (primaryIndexModificationOp.getOperation() == Kind.UPSERT) {
                        indexUpdate.setBeforeOpSecondaryKeyExprs(beforeOpSecondaryExpressions);
                        if (filteringFields != null) {
                            VariableReferenceExpression varRef =
                                    new VariableReferenceExpression(primaryIndexModificationOp.getBeforeOpFilterVar());
                            varRef.setSourceLocation(sourceLoc);
                            indexUpdate.setBeforeOpAdditionalFilteringExpression(
                                    new MutableObject<ILogicalExpression>(varRef));
                        }
                    }
                    indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));

                    // For array indexes we have no secondary keys to reference. We must add separate branches to
                    // first extract our keys.
                    if (index.getIndexType() == IndexType.ARRAY && !isBulkload) {
                        NestedTupleSourceOperator unnestSourceOp =
                                new NestedTupleSourceOperator(new MutableObject<>(indexUpdate));
                        unnestSourceOp.setSourceLocation(sourceLoc);
                        context.computeAndSetTypeEnvironmentForOperator(unnestSourceOp);
                        UnnestBranchCreator unnestSIDXBranch = buildUnnestBranch(unnestSourceOp, index, newRecordVar,
                                newMetaVar, recType, metaType, dataset.hasMetaPart());
                        unnestSIDXBranch.applyProjectOnly();

                        // If there exists a filter expression, add it to the top of our nested plan.
                        filterExpression = (primaryIndexModificationOp.getOperation() == Kind.UPSERT) ? null
                                : createAnyUnknownFilterExpression(unnestSIDXBranch.lastFieldVars,
                                        context.getOutputTypeEnvironment(unnestSIDXBranch.currentTop),
                                        index.getIndexDetails().isOverridingKeyFieldTypes());
                        if (filterExpression != null) {
                            unnestSIDXBranch.applyFilteringExpression(filterExpression);
                        }

                        // Finalize our nested plan.
                        ILogicalPlan unnestPlan = unnestSIDXBranch.buildBranch();
                        indexUpdate.getNestedPlans().add(unnestPlan);

                        // If we have an UPSERT, then create and add a branch to extract our old keys as well.
                        if (primaryIndexModificationOp.getOperation() == Kind.UPSERT) {
                            NestedTupleSourceOperator unnestBeforeSourceOp =
                                    new NestedTupleSourceOperator(new MutableObject<>(indexUpdate));
                            unnestBeforeSourceOp.setSourceLocation(sourceLoc);
                            context.computeAndSetTypeEnvironmentForOperator(unnestBeforeSourceOp);

                            List<LogicalVariable> beforeOpMetaVars =
                                    primaryIndexModificationOp.getBeforeOpAdditionalNonFilteringVars();
                            LogicalVariable beforeOpMetaVar = beforeOpMetaVars == null ? null : beforeOpMetaVars.get(0);
                            UnnestBranchCreator unnestBeforeSIDXBranch = buildUnnestBranch(unnestBeforeSourceOp, index,
                                    primaryIndexModificationOp.getBeforeOpRecordVar(), beforeOpMetaVar, recType,
                                    metaType, dataset.hasMetaPart());
                            unnestBeforeSIDXBranch.applyProjectOnly();
                            indexUpdate.getNestedPlans().add(unnestBeforeSIDXBranch.buildBranch());
                        }
                    } else if (index.getIndexType() == IndexType.ARRAY && isBulkload) {
                        // If we have a bulk load, we must sort the entire input by <SK, PK>. Do not use any
                        // nested plans here.
                        UnnestBranchCreator unnestSIDXBranch = buildUnnestBranch(currentTop, index, newRecordVar,
                                newMetaVar, recType, metaType, dataset.hasMetaPart());
                        unnestSIDXBranch.applyProjectDistinct(primaryIndexModificationOp.getPrimaryKeyExpressions(),
                                primaryIndexModificationOp.getAdditionalFilteringExpressions());
                        indexUpdate.getInputs().clear();
                        introduceNewOp(unnestSIDXBranch.currentTop, indexUpdate, true);

                        // Update the secondary expressions of our index.
                        secondaryExpressions = new ArrayList<>();
                        for (LogicalVariable var : unnestSIDXBranch.lastFieldVars) {
                            secondaryExpressions.add(new MutableObject<>(new VariableReferenceExpression(var)));
                        }
                        indexUpdate.setSecondaryKeyExprs(secondaryExpressions);

                        // Update the filter expression to include these new keys.
                        filterExpression = createAnyUnknownFilterExpression(unnestSIDXBranch.lastFieldVars,
                                context.getOutputTypeEnvironment(unnestSIDXBranch.currentTop),
                                index.getIndexDetails().isOverridingKeyFieldTypes());
                        indexUpdate.setFilterExpression(filterExpression);

                        if (replicateOp != null) {
                            // If we have a replicate, then update the replicate operator to include this branch.
                            replicateOp.getOutputs().add(new MutableObject<>(unnestSIDXBranch.currentBottom));
                            op0.getInputs().add(new MutableObject<ILogicalOperator>(indexUpdate));
                            continue;
                        }
                    }
                }
            } else {
                // Get type, dimensions and number of keys
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(index, secondaryKeyTypes.get(0),
                        secondaryKeyFields.get(0), recType);
                IAType spatialType = keyPairType.first;
                boolean isPointMBR =
                        spatialType.getTypeTag() == ATypeTag.POINT || spatialType.getTypeTag() == ATypeTag.POINT3D;
                int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
                int numKeys = (isPointMBR && isBulkload) ? dimension : dimension * 2;
                // Get variables and expressions
                List<LogicalVariable> keyVarList = new ArrayList<>();
                List<Mutable<ILogicalExpression>> keyExprList = new ArrayList<>();
                for (int i = 0; i < numKeys; i++) {
                    LogicalVariable keyVar = context.newVar();
                    keyVarList.add(keyVar);
                    AbstractFunctionCallExpression createMBR =
                            new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.CREATE_MBR));
                    createMBR.setSourceLocation(sourceLoc);
                    VariableReferenceExpression secondaryKeyVarRef =
                            new VariableReferenceExpression(secondaryKeyVars.get(0));
                    secondaryKeyVarRef.setSourceLocation(sourceLoc);
                    createMBR.getArguments().add(new MutableObject<ILogicalExpression>(secondaryKeyVarRef));
                    createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                            new ConstantExpression(new AsterixConstantValue(new AInt32(dimension)))));
                    createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                            new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
                    keyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
                }
                secondaryExpressions.clear();
                for (LogicalVariable secondaryKeyVar : keyVarList) {
                    VariableReferenceExpression secondaryKeyVarRef = new VariableReferenceExpression(secondaryKeyVar);
                    secondaryKeyVarRef.setSourceLocation(sourceLoc);
                    secondaryExpressions.add(new MutableObject<ILogicalExpression>(secondaryKeyVarRef));
                }
                if (isPointMBR && isBulkload) {
                    //for PointMBR optimization: see SecondaryRTreeOperationsHelper.buildLoadingJobSpec() and
                    //createFieldPermutationForBulkLoadOp(int) for more details.
                    for (LogicalVariable secondaryKeyVar : keyVarList) {
                        VariableReferenceExpression secondaryKeyVarRef =
                                new VariableReferenceExpression(secondaryKeyVar);
                        secondaryKeyVarRef.setSourceLocation(sourceLoc);
                        secondaryExpressions.add(new MutableObject<ILogicalExpression>(secondaryKeyVarRef));
                    }
                }
                AssignOperator assignCoordinates = new AssignOperator(keyVarList, keyExprList);
                assignCoordinates.setSourceLocation(sourceLoc);
                assignCoordinates.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
                context.computeAndSetTypeEnvironmentForOperator(assignCoordinates);
                replicateOutput = assignCoordinates;
                boolean forceFilter = keyPairType.second;
                Mutable<ILogicalExpression> filterExpression = createAnyUnknownFilterExpression(keyVarList,
                        context.getOutputTypeEnvironment(assignCoordinates), forceFilter);
                AssignOperator originalAssignCoordinates = null;
                Mutable<ILogicalExpression> beforeOpFilterExpression = null;
                // We do something similar for beforeOp key if the operation is an upsert
                if (primaryIndexModificationOp.getOperation() == Kind.UPSERT) {
                    List<LogicalVariable> originalKeyVarList = new ArrayList<>();
                    List<Mutable<ILogicalExpression>> originalKeyExprList = new ArrayList<>();
                    // we don't do any filtering since nulls are expected here and there
                    for (int i = 0; i < numKeys; i++) {
                        LogicalVariable keyVar = context.newVar();
                        originalKeyVarList.add(keyVar);
                        AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                                FunctionUtil.getFunctionInfo(BuiltinFunctions.CREATE_MBR));
                        createMBR.setSourceLocation(sourceLoc);
                        createMBR.getArguments().add(
                                new MutableObject<>(beforeOpSecondaryExpressions.get(0).getValue().cloneExpression()));
                        createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                                new ConstantExpression(new AsterixConstantValue(new AInt32(dimension)))));
                        createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                                new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
                        originalKeyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
                    }
                    beforeOpSecondaryExpressions.clear();
                    for (LogicalVariable secondaryKeyVar : originalKeyVarList) {
                        VariableReferenceExpression secondaryKeyVarRef =
                                new VariableReferenceExpression(secondaryKeyVar);
                        secondaryKeyVarRef.setSourceLocation(sourceLoc);
                        beforeOpSecondaryExpressions.add(new MutableObject<ILogicalExpression>(secondaryKeyVarRef));
                    }
                    originalAssignCoordinates = new AssignOperator(originalKeyVarList, originalKeyExprList);
                    originalAssignCoordinates.setSourceLocation(sourceLoc);
                    originalAssignCoordinates.getInputs().add(new MutableObject<ILogicalOperator>(assignCoordinates));
                    context.computeAndSetTypeEnvironmentForOperator(originalAssignCoordinates);
                    beforeOpFilterExpression = createAnyUnknownFilterExpression(originalKeyVarList,
                            context.getOutputTypeEnvironment(originalAssignCoordinates), forceFilter);
                }
                DataSourceIndex dataSourceIndex = new DataSourceIndex(index, database, dataverseName, datasetName, mp);
                indexUpdate = new IndexInsertDeleteUpsertOperator(dataSourceIndex,
                        OperatorManipulationUtil
                                .cloneExpressions(primaryIndexModificationOp.getPrimaryKeyExpressions()),
                        secondaryExpressions, filterExpression, beforeOpFilterExpression,
                        primaryIndexModificationOp.getOperation(), primaryIndexModificationOp.isBulkload(),
                        primaryIndexModificationOp.getAdditionalNonFilteringExpressions() == null ? 0
                                : primaryIndexModificationOp.getAdditionalNonFilteringExpressions().size());
                indexUpdate.setSourceLocation(sourceLoc);
                indexUpdate.setAdditionalFilteringExpressions(
                        OperatorManipulationUtil.cloneExpressions(filteringExpressions));
                if (primaryIndexModificationOp.getOperation() == Kind.UPSERT) {
                    // set before op secondary key expressions
                    if (filteringFields != null) {
                        VariableReferenceExpression varRef =
                                new VariableReferenceExpression(primaryIndexModificationOp.getBeforeOpFilterVar());
                        varRef.setSourceLocation(sourceLoc);
                        indexUpdate.setBeforeOpAdditionalFilteringExpression(
                                new MutableObject<ILogicalExpression>(varRef));
                    }
                    // set filtering expressions
                    indexUpdate.setBeforeOpSecondaryKeyExprs(beforeOpSecondaryExpressions);
                    // assign --> assign beforeOp values --> secondary index upsert
                    indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(originalAssignCoordinates));
                } else {
                    indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(assignCoordinates));
                }
            }

            if (primaryIndexModificationOp.getOperation() == Kind.UPSERT) {
                indexUpdate.setOperationExpr(new MutableObject<>(
                        new VariableReferenceExpression(primaryIndexModificationOp.getOperationVar())));
            }

            context.computeAndSetTypeEnvironmentForOperator(indexUpdate);
            if (!primaryIndexModificationOp.isBulkload() || secondaryIndexTotalCnt == 1) {
                currentTop = indexUpdate;
            } else {
                replicateOp.getOutputs().add(new MutableObject<>(replicateOutput));

                /* special treatment for bulk load with the existence of secondary primary index.
                 * the branch coming out of the replicate operator and feeding the index will not have the usual
                 * "blocking" sort operator since tuples are already sorted. We mark the materialization flag for that
                 * branch to make it blocking. Without "blocking", the activity cluster graph would be messed up
                 */
                if (index.isPrimaryKeyIndex()) {
                    int positionOfSecondaryPrimaryIndex = replicateOp.getOutputs().size() - 1;
                    replicateOp.getOutputMaterializationFlags()[positionOfSecondaryPrimaryIndex] = true;
                }
            }
            if (primaryIndexModificationOp.isBulkload()) {
                // For bulk load, we connect all fanned out insert operator to a single SINK operator
                op0.getInputs().add(new MutableObject<ILogicalOperator>(indexUpdate));
            }
        }

        if (!primaryIndexModificationOp.isBulkload()) {
            // If this is an upsert, we need to
            // Remove the current input to the SINK operator (It is actually already removed above)
            op0.getInputs().clear();
            // Connect the last index update to the SINK
            op0.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
        }
        return true;
    }

    private UnnestBranchCreator buildUnnestBranch(ILogicalOperator unnestSourceOp, Index index,
            LogicalVariable recordVar, LogicalVariable metaVar, ARecordType recType, ARecordType metaType,
            boolean hasMetaPart) throws AlgebricksException {
        Index.ArrayIndexDetails arrayIndexDetails = (Index.ArrayIndexDetails) index.getIndexDetails();

        // Get the record variable associated with our record path.
        int sourceIndicatorForBaseRecord = arrayIndexDetails.getElementList().get(0).getSourceIndicator();
        LogicalVariable sourceVarForBaseRecord = hasMetaPart
                ? ((sourceIndicatorForBaseRecord == Index.RECORD_INDICATOR) ? recordVar : metaVar) : recordVar;
        UnnestBranchCreator branchCreator = new UnnestBranchCreator(index, sourceVarForBaseRecord, unnestSourceOp);

        Set<LogicalVariable> secondaryKeyVars = new LinkedHashSet<>();
        for (Index.ArrayIndexElement workingElement : arrayIndexDetails.getElementList()) {
            int sourceIndicator = workingElement.getSourceIndicator();
            ARecordType recordType =
                    hasMetaPart ? ((sourceIndicator == Index.RECORD_INDICATOR) ? recType : metaType) : recType;

            boolean isOpenOrNestedField;
            if (workingElement.getUnnestList().isEmpty()) {
                // We have an atomic element (i.e. we have a composite array index).
                List<String> atomicFieldName = workingElement.getProjectList().get(0);
                isOpenOrNestedField =
                        (atomicFieldName.size() != 1) || !recordType.isClosedField(atomicFieldName.get(0));

                LogicalVariable newVar = context.newVar();
                VariableReferenceExpression varRef = new VariableReferenceExpression(sourceVarForBaseRecord);
                varRef.setSourceLocation(sourceLoc);
                AbstractFunctionCallExpression newVarRef = (isOpenOrNestedField)
                        ? getFieldAccessFunction(new MutableObject<>(varRef),
                                recordType.getFieldIndex(atomicFieldName.get(0)), atomicFieldName)
                        : getFieldAccessFunction(new MutableObject<>(varRef), -1, atomicFieldName);
                IAType fieldType = recordType.getSubFieldType(atomicFieldName);
                if (fieldType == null) {
                    newVarRef = castFunction(
                            index.isEnforced() ? BuiltinFunctions.CAST_TYPE : BuiltinFunctions.CAST_TYPE_LAX,
                            workingElement.getTypeList().get(0), newVarRef, sourceLoc);
                }
                // Add an assign on top to extract the atomic element.
                AssignOperator newAssignOp = new AssignOperator(newVar, new MutableObject<>(newVarRef));
                newAssignOp.setSourceLocation(sourceLoc);
                branchCreator.currentTop = introduceNewOp(branchCreator.currentTop, newAssignOp, true);
                secondaryKeyVars.add(newVar);
                if (branchCreator.currentBottom == null) {
                    branchCreator.currentBottom = branchCreator.currentTop;
                }

            } else {
                // We have an array element.  Walk the array path.
                List<String> flatFirstFieldName = ArrayIndexUtil.getFlattenedKeyFieldNames(
                        workingElement.getUnnestList(), workingElement.getProjectList().get(0));
                List<Boolean> firstUnnestFlags = ArrayIndexUtil.getUnnestFlags(workingElement.getUnnestList(),
                        workingElement.getProjectList().get(0));
                ArrayIndexUtil.walkArrayPath(index, workingElement, recordType, flatFirstFieldName, firstUnnestFlags,
                        branchCreator);
                secondaryKeyVars.add(branchCreator.lastFieldVars.get(0));

                // For all other elements in the PROJECT list, add an assign.
                for (int j = 1; j < workingElement.getProjectList().size(); j++) {
                    LogicalVariable newVar = context.newVar();
                    ILogicalExpression newVarRef =
                            getFieldAccessFunction(new MutableObject<>(branchCreator.createLastRecordVarRef()), -1,
                                    workingElement.getProjectList().get(j));
                    newVarRef = createCastExpressionForArrayIndex(newVarRef, recordType, index, workingElement, j);
                    AssignOperator newAssignOp = new AssignOperator(newVar, new MutableObject<>(newVarRef));
                    newAssignOp.setSourceLocation(sourceLoc);
                    branchCreator.currentTop = introduceNewOp(branchCreator.currentTop, newAssignOp, true);
                    secondaryKeyVars.add(newVar);
                }
            }
        }

        // Update the variables we are to use for the head operators.
        branchCreator.lastFieldVars.clear();
        branchCreator.lastFieldVars.addAll(secondaryKeyVars);
        return branchCreator;
    }

    private LogicalVariable getRecordVar(AbstractLogicalOperator inputOp, ILogicalExpression recordExpr,
            int expectedRecordIndex) throws AlgebricksException {
        if (exprIsRecord(context.getOutputTypeEnvironment(inputOp), recordExpr)) {
            return ((VariableReferenceExpression) recordExpr).getVariableReference();
        } else {
            /**
             * For the case primary key-assignment expressions are constant
             * expressions, find assign op that creates record to be
             * inserted/deleted.
             */
            FunctionIdentifier fid = null;
            AbstractLogicalOperator currentInputOp = inputOp;
            while (fid != BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR) {
                if (currentInputOp.getInputs().isEmpty()) {
                    return null;
                }
                currentInputOp = (AbstractLogicalOperator) currentInputOp.getInputs().get(0).getValue();
                if (currentInputOp.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
                    continue;
                }
                AssignOperator assignOp = (AssignOperator) currentInputOp;
                ILogicalExpression assignExpr = assignOp.getExpressions().get(expectedRecordIndex).getValue();
                if (assignExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    ScalarFunctionCallExpression funcExpr = (ScalarFunctionCallExpression) assignOp.getExpressions()
                            .get(expectedRecordIndex).getValue();
                    fid = funcExpr.getFunctionIdentifier();
                }
            }
            return ((AssignOperator) currentInputOp).getVariables().get(0);
        }
    }

    private boolean exprIsRecord(IVariableTypeEnvironment typeEnvironment, ILogicalExpression recordExpr)
            throws AlgebricksException {
        if (recordExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            IAType type = (IAType) typeEnvironment.getType(recordExpr);
            return type != null && type.getTypeTag() == ATypeTag.OBJECT;
        }
        return false;
    }

    private ILogicalOperator injectFieldAccessesForIndexes(Dataset dataset, List<Index> indexes,
            Map<IndexFieldId, LogicalVariable> fieldAccessVars, ARecordType recType, ARecordType metaType,
            LogicalVariable recordVar, LogicalVariable metaVar, ILogicalOperator currentTop, boolean afterOp)
            throws AlgebricksException {
        List<LogicalVariable> vars = new ArrayList<>();
        List<Mutable<ILogicalExpression>> exprs = new ArrayList<>();
        SourceLocation sourceLoc = currentTop.getSourceLocation();
        for (Index index : indexes) {
            if (index.isPrimaryIndex() || index.getIndexType() == IndexType.ARRAY) {
                // Array indexes require UNNESTs, which must be handled after the PIDX op.
                continue;
            }
            List<List<String>> skNames;
            List<IAType> skTypes;
            List<Integer> indicators;
            switch (Index.IndexCategory.of(index.getIndexType())) {
                case VALUE:
                    Index.ValueIndexDetails valueIndexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
                    skNames = valueIndexDetails.getKeyFieldNames();
                    skTypes = valueIndexDetails.getKeyFieldTypes();
                    indicators = valueIndexDetails.getKeyFieldSourceIndicators();
                    break;
                case TEXT:
                    Index.TextIndexDetails textIndexDetails = (Index.TextIndexDetails) index.getIndexDetails();
                    skNames = textIndexDetails.getKeyFieldNames();
                    skTypes = textIndexDetails.getKeyFieldTypes();
                    indicators = textIndexDetails.getKeyFieldSourceIndicators();
                    break;
                default:
                    throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE,
                            String.valueOf(index.getIndexType()));
            }
            for (int i = 0; i < skNames.size(); i++) {
                List<String> skName = skNames.get(i);
                Integer skSrc = indicators.get(i);
                IAType skType = skTypes.get(i);

                ARecordType sourceType = dataset.hasMetaPart()
                        ? skSrc.intValue() == Index.RECORD_INDICATOR ? recType : metaType : recType;
                LogicalVariable sourceVar = dataset.hasMetaPart()
                        ? skSrc.intValue() == Index.RECORD_INDICATOR ? recordVar : metaVar : recordVar;

                IAType fieldType = sourceType.getSubFieldType(skName);
                IndexFieldId indexFieldId = createIndexFieldId(index, skName, skType, skSrc, sourceType, sourceLoc);
                if (fieldAccessVars.containsKey(indexFieldId)) {
                    // already handled in a different index
                    continue;
                }
                // create record variable ref
                VariableReferenceExpression varRef = new VariableReferenceExpression(sourceVar);
                varRef.setSourceLocation(sourceLoc);

                AbstractFunctionCallExpression theFieldAccessFunc;
                LogicalVariable fieldVar = context.newVar();
                if (fieldType == null) {
                    // Open field. must prevent inlining to maintain the cast before the primaryOp and
                    // make handling of records with incorrect value type for this field easier and cleaner
                    context.addNotToBeInlinedVar(fieldVar);
                    // create field access
                    AbstractFunctionCallExpression fieldAccessFunc =
                            getFieldAccessFunction(new MutableObject<>(varRef), -1, indexFieldId.fieldName);
                    // create cast
                    theFieldAccessFunc = createCastExpression(index, skType, fieldAccessFunc, sourceLoc,
                            indexFieldId.funId, indexFieldId.extraArg);
                } else {
                    // Get the desired field position
                    int pos = indexFieldId.fieldName.size() > 1 ? -1
                            : sourceType.getFieldIndex(indexFieldId.fieldName.get(0));
                    // Field not found --> This is either an open field or a nested field. it can't be accessed by index
                    theFieldAccessFunc =
                            getFieldAccessFunction(new MutableObject<>(varRef), pos, indexFieldId.fieldName);
                    // check IndexUtil.castDefaultNull(index), too, because we always want to cast even if
                    // the overriding type is the same as the overridden type (this is for the case where overriding
                    // the type of closed field is allowed)
                    // e.g. field "a" is a string in the dataset ds; CREATE INDEX .. ON ds(a:string) CAST (DEFAULT NULL)
                    if (IndexUtil.castDefaultNull(index)) {
                        theFieldAccessFunc = castConstructorFunction(indexFieldId.funId, indexFieldId.extraArg,
                                theFieldAccessFunc, sourceLoc);
                    }
                }
                vars.add(fieldVar);
                exprs.add(new MutableObject<>(theFieldAccessFunc));
                fieldAccessVars.put(indexFieldId, fieldVar);
            }
        }
        if (!vars.isEmpty()) {
            // AssignOperator assigns secondary keys to their vars
            AssignOperator castedFieldAssignOperator = new AssignOperator(vars, exprs);
            castedFieldAssignOperator.setSourceLocation(sourceLoc);
            return introduceNewOp(currentTop, castedFieldAssignOperator, afterOp);
        }
        return currentTop;
    }

    private static IndexFieldId createIndexFieldId(Index index, List<String> skName, IAType skType, Integer skSrc,
            ARecordType sourceType, SourceLocation srcLoc) throws AlgebricksException {
        IAType fieldType = sourceType.getSubFieldType(skName);
        FunctionIdentifier skFun = null;
        IAObject fmtArg = null;
        Pair<FunctionIdentifier, IAObject> castExpr;
        if (fieldType == null) {
            // open field
            castExpr = getCastExpression(index, skType, srcLoc);
            skFun = castExpr.first;
            fmtArg = castExpr.second;
        } else {
            // closed field
            if (IndexUtil.castDefaultNull(index)) {
                castExpr = IndexUtil.getTypeConstructorDefaultNull(index, skType, srcLoc);
                skFun = castExpr.first;
                fmtArg = castExpr.second;
            }
        }
        return new IndexFieldId(skSrc, skName, skType.getTypeTag(), skFun, fmtArg);
    }

    private static Pair<FunctionIdentifier, IAObject> getCastExpression(Index index, IAType skType,
            SourceLocation srcLoc) throws AlgebricksException {
        if (IndexUtil.castDefaultNull(index)) {
            return IndexUtil.getTypeConstructorDefaultNull(index, skType, srcLoc);
        } else if (index.isEnforced()) {
            return new Pair<>(BuiltinFunctions.CAST_TYPE, null);
        } else {
            return new Pair<>(BuiltinFunctions.CAST_TYPE_LAX, null);
        }
    }

    private AbstractFunctionCallExpression createCastExpression(Index index, IAType targetType,
            AbstractFunctionCallExpression inputExpr, SourceLocation sourceLoc, FunctionIdentifier castFun,
            IAObject fmtArg) throws CompilationException {
        ScalarFunctionCallExpression castExpr;
        if (IndexUtil.castDefaultNull(index)) {
            castExpr = castConstructorFunction(castFun, fmtArg, inputExpr, sourceLoc);
        } else {
            castExpr = castFunction(castFun, targetType, inputExpr, sourceLoc);
        }
        return castExpr;
    }

    private ScalarFunctionCallExpression castFunction(FunctionIdentifier castFun, IAType requiredType,
            ILogicalExpression inputExpr, SourceLocation sourceLoc) throws CompilationException {
        BuiltinFunctionInfo castInfo = BuiltinFunctions.getBuiltinFunctionInfo(castFun);
        ScalarFunctionCallExpression castExpr = new ScalarFunctionCallExpression(castInfo);
        castExpr.setSourceLocation(sourceLoc);
        castExpr.getArguments().add(new MutableObject<>(inputExpr));
        TypeCastUtils.setRequiredAndInputTypes(castExpr, requiredType, BuiltinType.ANY);
        return castExpr;
    }

    private ScalarFunctionCallExpression castConstructorFunction(FunctionIdentifier typeConstructorFun, IAObject fmt,
            AbstractFunctionCallExpression inputExpr, SourceLocation srcLoc) {
        BuiltinFunctionInfo typeConstructorInfo = BuiltinFunctions.getBuiltinFunctionInfo(typeConstructorFun);
        ScalarFunctionCallExpression constructorExpr = new ScalarFunctionCallExpression(typeConstructorInfo);
        constructorExpr.getArguments().add(new MutableObject<>(inputExpr));
        // add the format argument if specified
        if (fmt != null) {
            ConstantExpression fmtExpr = new ConstantExpression(new AsterixConstantValue(fmt));
            fmtExpr.setSourceLocation(srcLoc);
            constructorExpr.getArguments().add(new MutableObject<>(fmtExpr));
        }
        constructorExpr.setSourceLocation(srcLoc);
        return constructorExpr;
    }

    private ILogicalExpression createCastExpressionForArrayIndex(ILogicalExpression varRef, ARecordType recordType,
            Index index, Index.ArrayIndexElement workingElement, int fieldPos) throws AlgebricksException {
        IAType fieldType = ArrayIndexUtil.getSubFieldType(recordType, workingElement.getUnnestList(),
                workingElement.getProjectList().get(fieldPos));
        if (fieldType != null) {
            return varRef;
        } else {
            return castFunction(index.isEnforced() ? BuiltinFunctions.CAST_TYPE : BuiltinFunctions.CAST_TYPE_LAX,
                    workingElement.getTypeList().get(fieldPos), varRef, sourceLoc);
        }
    }

    private ILogicalOperator introduceNewOp(ILogicalOperator currentTopOp, ILogicalOperator newOp, boolean afterOp)
            throws AlgebricksException {
        if (afterOp) {
            newOp.getInputs().add(new MutableObject<>(currentTopOp));
            context.computeAndSetTypeEnvironmentForOperator(newOp);
            return newOp;
        } else {
            newOp.getInputs().addAll(currentTopOp.getInputs());
            currentTopOp.getInputs().clear();
            currentTopOp.getInputs().add(new MutableObject<>(newOp));
            context.computeAndSetTypeEnvironmentForOperator(newOp);
            context.computeAndSetTypeEnvironmentForOperator(currentTopOp);
            return currentTopOp;
        }
    }

    private AbstractFunctionCallExpression getFieldAccessFunction(Mutable<ILogicalExpression> varRef, int fieldPos,
            List<String> fieldName) {
        if (fieldName.size() == 1 && fieldPos != -1) {
            Mutable<ILogicalExpression> indexRef =
                    new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(fieldPos))));
            ScalarFunctionCallExpression fnExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_INDEX), varRef, indexRef);
            fnExpr.setSourceLocation(sourceLoc);
            return fnExpr;

        } else {
            ScalarFunctionCallExpression func;
            if (fieldName.size() > 1) {
                IAObject fieldList = stringListToAOrderedList(fieldName);
                Mutable<ILogicalExpression> fieldRef = constantToMutableLogicalExpression(fieldList);
                // Create an expression for the nested case
                func = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_NESTED), varRef, fieldRef);
            } else {
                IAObject fieldList = new AString(fieldName.get(0));
                Mutable<ILogicalExpression> fieldRef = constantToMutableLogicalExpression(fieldList);
                // Create an expression for the open field case (By name)
                func = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME), varRef, fieldRef);
            }
            func.setSourceLocation(sourceLoc);
            return func;
        }
    }

    private static AOrderedList stringListToAOrderedList(List<String> fields) {
        AOrderedList fieldList = new AOrderedList(new AOrderedListType(BuiltinType.ASTRING, null));
        for (int i = 0; i < fields.size(); i++) {
            fieldList.add(new AString(fields.get(i)));
        }
        return fieldList;
    }

    private static Mutable<ILogicalExpression> constantToMutableLogicalExpression(IAObject constantObject) {
        return new MutableObject<>(new ConstantExpression(new AsterixConstantValue(constantObject)));
    }

    private Mutable<ILogicalExpression> createFilterExpression(Index index, List<LogicalVariable> secondaryKeyVars,
            IVariableTypeEnvironment typeEnv, boolean forceFilter) throws AlgebricksException {
        IndexType indexType = index.getIndexType();
        if (indexType == IndexType.BTREE) {
            if (index.isPrimaryKeyIndex()) {
                return createAnyUnknownFilterExpression(secondaryKeyVars, typeEnv, forceFilter);
            } else {
                Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
                boolean excludeUnknown = indexDetails.getExcludeUnknownKey().getOrElse(false);
                return createAllUnknownFilterExpression(secondaryKeyVars, typeEnv, forceFilter, excludeUnknown);
            }
        } else {
            // inverted index && array index
            return createAnyUnknownFilterExpression(secondaryKeyVars, typeEnv, forceFilter);
        }
    }

    private Mutable<ILogicalExpression> createAnyUnknownFilterExpression(List<LogicalVariable> secondaryKeyVars,
            IVariableTypeEnvironment typeEnv, boolean forceFilter) throws AlgebricksException {
        return createFilterExpression(secondaryKeyVars, typeEnv, forceFilter, true, BuiltinFunctions.AND);
    }

    private Mutable<ILogicalExpression> createAllUnknownFilterExpression(List<LogicalVariable> secondaryKeyVars,
            IVariableTypeEnvironment typeEnv, boolean forceFilter, boolean excludeUnknownKey)
            throws AlgebricksException {
        return createFilterExpression(secondaryKeyVars, typeEnv, forceFilter, excludeUnknownKey, BuiltinFunctions.OR);
    }

    private Mutable<ILogicalExpression> createFilterExpression(List<LogicalVariable> secondaryKeyVars,
            IVariableTypeEnvironment typeEnv, boolean forceFilter, boolean excludeUnknownKey,
            FunctionIdentifier combiner) throws AlgebricksException {
        if (!excludeUnknownKey) {
            return null;
        }
        List<Mutable<ILogicalExpression>> filterExpressions = new ArrayList<>();
        // Add 'is not null' to all nullable secondary index keys as a filtering condition
        for (LogicalVariable secondaryKeyVar : secondaryKeyVars) {
            IAType secondaryKeyType = (IAType) typeEnv.getVarType(secondaryKeyVar);
            if (!NonTaggedFormatUtil.isOptional(secondaryKeyType) && !forceFilter) {
                continue;
            }
            VariableReferenceExpression secondaryKeyVarRef = new VariableReferenceExpression(secondaryKeyVar);
            secondaryKeyVarRef.setSourceLocation(sourceLoc);
            ScalarFunctionCallExpression isUnknownFuncExpr =
                    new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.IS_UNKNOWN),
                            new MutableObject<ILogicalExpression>(secondaryKeyVarRef));
            isUnknownFuncExpr.setSourceLocation(sourceLoc);
            ScalarFunctionCallExpression notFuncExpr =
                    new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.NOT),
                            new MutableObject<ILogicalExpression>(isUnknownFuncExpr));
            notFuncExpr.setSourceLocation(sourceLoc);
            filterExpressions.add(new MutableObject<ILogicalExpression>(notFuncExpr));
        }
        // No nullable secondary keys.
        if (filterExpressions.isEmpty()) {
            return null;
        }
        Mutable<ILogicalExpression> filterExpression;
        if (filterExpressions.size() > 1) {
            // Combine the conditions
            ScalarFunctionCallExpression combinerExpr = new ScalarFunctionCallExpression(
                    BuiltinFunctions.getBuiltinFunctionInfo(combiner), filterExpressions);
            combinerExpr.setSourceLocation(sourceLoc);
            filterExpression = new MutableObject<>(combinerExpr);
        } else {
            filterExpression = filterExpressions.get(0);
        }
        return filterExpression;
    }

    /**
     * Builds the nested plan required for array index maintenance.
     */
    private class UnnestBranchCreator implements ArrayIndexUtil.TypeTrackerCommandExecutor {
        private final List<LogicalVariable> lastFieldVars;
        private LogicalVariable lastRecordVar;
        private ILogicalOperator currentTop, currentBottom = null;
        private final Index index;

        public UnnestBranchCreator(Index index, LogicalVariable recordVar, ILogicalOperator sourceOperator) {
            this.index = index;
            this.lastRecordVar = recordVar;
            this.currentTop = sourceOperator;
            this.lastFieldVars = new ArrayList<>();
        }

        public ILogicalPlan buildBranch() {
            return new ALogicalPlanImpl(new MutableObject<>(currentTop));
        }

        public VariableReferenceExpression createLastRecordVarRef() {
            VariableReferenceExpression varRef = new VariableReferenceExpression(lastRecordVar);
            varRef.setSourceLocation(sourceLoc);
            return varRef;
        }

        public final void applyProjectOnly() throws AlgebricksException {
            List<LogicalVariable> projectVars = new ArrayList<>(this.lastFieldVars);
            ProjectOperator projectOperator = new ProjectOperator(projectVars);
            projectOperator.setSourceLocation(sourceLoc);
            this.currentTop = introduceNewOp(currentTop, projectOperator, true);
        }

        @SafeVarargs
        public final void applyProjectDistinct(List<Mutable<ILogicalExpression>>... auxiliaryExpressions)
                throws AlgebricksException {
            // Apply the following: PROJECT(SK, AK) --> (ORDER (SK, AK)) implicitly --> DISTINCT (SK, AK) .
            List<LogicalVariable> projectVars = new ArrayList<>(this.lastFieldVars);
            List<Mutable<ILogicalExpression>> distinctVarRefs =
                    OperatorManipulationUtil.createVariableReferences(projectVars, sourceLoc);

            // If we have any additional expressions to be added to our index, append them here.
            if (auxiliaryExpressions.length > 0) {
                for (List<Mutable<ILogicalExpression>> exprList : auxiliaryExpressions) {
                    if (exprList != null) {
                        // Sanity check: ensure that we only have variable references.
                        if (exprList.stream().anyMatch(
                                e -> !e.getValue().getExpressionTag().equals(LogicalExpressionTag.VARIABLE))) {
                            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc,
                                    "Given auxiliary expression list contains non-variable reference expressions. We"
                                            + " cannot apply DISTINCT to this expression at this stage.");
                        }

                        distinctVarRefs.addAll(OperatorManipulationUtil.cloneExpressions(exprList));
                        for (Mutable<ILogicalExpression> e : OperatorManipulationUtil.cloneExpressions(exprList)) {
                            projectVars.add(((VariableReferenceExpression) e.getValue()).getVariableReference());
                        }
                    }
                }
            }

            ProjectOperator projectOperator = new ProjectOperator(projectVars);
            projectOperator.setSourceLocation(sourceLoc);
            this.currentTop = introduceNewOp(currentTop, projectOperator, true);
            DistinctOperator distinctOperator = new DistinctOperator(distinctVarRefs);
            distinctOperator.setSourceLocation(sourceLoc);
            this.currentTop = introduceNewOp(currentTop, distinctOperator, true);
        }

        public void applyFilteringExpression(Mutable<ILogicalExpression> filterExpression) throws AlgebricksException {
            SelectOperator selectOperator = new SelectOperator(filterExpression);
            selectOperator.setSourceLocation(sourceLoc);
            this.currentTop = introduceNewOp(currentTop, selectOperator, true);
        }

        @Override
        public void executeActionOnEachArrayStep(ARecordType startingStepRecordType, IAType workingType,
                List<String> fieldName, boolean isFirstArrayStep, boolean isLastUnnestInIntermediateStep)
                throws AlgebricksException {
            // Get the field we want to UNNEST from our record.
            ILogicalExpression accessToUnnestVar;
            accessToUnnestVar = (startingStepRecordType != null)
                    ? getFieldAccessFunction(new MutableObject<>(createLastRecordVarRef()),
                            startingStepRecordType.getFieldIndex(fieldName.get(0)), fieldName)
                    : getFieldAccessFunction(new MutableObject<>(createLastRecordVarRef()), -1, fieldName);
            UnnestingFunctionCallExpression scanCollection = new UnnestingFunctionCallExpression(
                    BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SCAN_COLLECTION),
                    Collections.singletonList(new MutableObject<>(accessToUnnestVar)));
            scanCollection.setReturnsUniqueValues(false);
            scanCollection.setSourceLocation(sourceLoc);
            LogicalVariable unnestVar = context.newVar();
            this.lastFieldVars.add(unnestVar);

            UnnestOperator unnestOp = new UnnestOperator(unnestVar, new MutableObject<>(scanCollection));
            unnestOp.setSourceLocation(sourceLoc);
            this.currentTop = introduceNewOp(currentTop, unnestOp, true);
            if (isFirstArrayStep && this.currentBottom == null) {
                this.currentBottom = unnestOp;
            }

            if (isLastUnnestInIntermediateStep) {
                // This is the last UNNEST before the next array step. Update our record variable.
                this.lastRecordVar = unnestVar;
                this.lastFieldVars.clear();
            }
        }

        @Override
        public void executeActionOnFinalArrayStep(Index.ArrayIndexElement workingElement, ARecordType baseRecordType,
                ARecordType startingStepRecordType, List<String> fieldName, boolean isNonArrayStep,
                boolean requiresOnlyOneUnnest) throws AlgebricksException {
            // If the final value is nested inside a record, add an additional ASSIGN.
            ILogicalExpression accessToFinalVar;
            if (!isNonArrayStep) {
                accessToFinalVar = createCastExpressionForArrayIndex(createLastRecordVarRef(), baseRecordType, index,
                        workingElement, 0);
            } else {
                // Create the function to access our final field.
                accessToFinalVar = (startingStepRecordType != null)
                        ? getFieldAccessFunction(new MutableObject<>(createLastRecordVarRef()),
                                startingStepRecordType.getFieldIndex(fieldName.get(0)), fieldName)
                        : getFieldAccessFunction(new MutableObject<>(createLastRecordVarRef()), -1, fieldName);
                accessToFinalVar =
                        createCastExpressionForArrayIndex(accessToFinalVar, baseRecordType, index, workingElement, 0);
            }
            LogicalVariable finalVar = context.newVar();
            this.lastFieldVars.add(finalVar);
            AssignOperator assignOperator = new AssignOperator(finalVar, new MutableObject<>(accessToFinalVar));
            assignOperator.setSourceLocation(sourceLoc);
            this.currentTop = introduceNewOp(currentTop, assignOperator, true);
        }
    }

    private static class IndexFieldId {
        private final int indicator;
        private final List<String> fieldName;
        private final ATypeTag fieldType;
        private final FunctionIdentifier funId;
        private final IAObject extraArg; // currently, only for datetime constructor functions with the format arg

        private IndexFieldId(int indicator, List<String> fieldName, ATypeTag fieldType, FunctionIdentifier funId,
                IAObject extraArg) {
            this.indicator = indicator;
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.funId = funId;
            this.extraArg = extraArg;
        }

        @Override
        public int hashCode() {
            int result = indicator;
            result = 31 * result + fieldName.hashCode();
            result = 31 * result + fieldType.hashCode();
            result = 31 * result + Objects.hashCode(funId);
            result = 31 * result + Objects.hashCode(extraArg);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IndexFieldId that = (IndexFieldId) o;
            return indicator == that.indicator && Objects.equals(fieldName, that.fieldName)
                    && fieldType == that.fieldType && Objects.equals(funId, that.funId)
                    && Objects.equals(extraArg, that.extraArg);
        }
    }
}
