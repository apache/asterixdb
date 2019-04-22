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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.asterix.algebra.operators.CommitOperator;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceIndex;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
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
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator.Kind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * This rule matches the pattern:
 * assign --> insert-delete-upsert --> sink
 * and produces
 * assign --> insert-delete-upsert --> *(secondary indexes index-insert-delete-upsert) --> sink
 */
public class IntroduceSecondaryIndexInsertDeleteRule implements IAlgebraicRewriteRule {

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
        SourceLocation sourceLoc = primaryIndexModificationOp.getSourceLocation();

        /**
         * inputOp is the assign operator which extracts primary keys from the input
         * variables (record or meta)
         */
        AbstractLogicalOperator inputOp =
                (AbstractLogicalOperator) primaryIndexModificationOp.getInputs().get(0).getValue();
        newRecordVar = getRecordVar(context, inputOp, newRecordExpr, 0);
        if (newMetaExprs != null && !newMetaExprs.isEmpty()) {
            if (newMetaExprs.size() > 1) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Number of meta records can't be more than 1. Number of meta records found = "
                                + newMetaExprs.size());
            }
            newMetaVar = getRecordVar(context, inputOp, newMetaExprs.get(0).getValue(), 1);
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
        String dataverseName = datasetSource.getId().getDataverseName();
        String datasetName = datasetSource.getId().getDatasourceName();
        Dataset dataset = mp.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                    dataverseName);
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return false;
        }

        // Create operators for secondary index insert / delete.
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType = mp.findType(dataset.getItemTypeDataverseName(), itemTypeName);
        if (itemType.getTypeTag() != ATypeTag.OBJECT) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Only record types can be indexed.");
        }
        ARecordType recType = (ARecordType) itemType;
        // meta type
        ARecordType metaType = null;
        if (dataset.hasMetaPart()) {
            metaType = (ARecordType) mp.findType(dataset.getMetaItemTypeDataverseName(), dataset.getMetaItemTypeName());
        }
        List<Index> indexes = mp.getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName());
        if (primaryIndexModificationOp.getOperation() == Kind.INSERT && !primaryIndexModificationOp.isBulkload()) {
            // for insert, primary key index is handled together when primary index
            indexes = indexes.stream().filter(index -> !index.isPrimaryKeyIndex()).collect(Collectors.toList());
        }
        // Set the top operator pointer to the primary IndexInsertDeleteOperator
        ILogicalOperator currentTop = primaryIndexModificationOp;
        boolean hasSecondaryIndex = false;

        // Put an n-gram or a keyword index in the later stage of index-update,
        // since TokenizeOperator needs to be involved.
        Collections.sort(indexes, (o1, o2) -> o1.getIndexType().ordinal() - o2.getIndexType().ordinal());

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
        if (secondaryIndexTotalCnt > 1 && primaryIndexModificationOp.isBulkload()) {
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
            injectFieldAccessesForIndexes(context, dataset, indexes, fieldVarsForNewRecord, recType, metaType,
                    newRecordVar, newMetaVar, primaryIndexModificationOp, false);
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
            currentTop = injectFieldAccessesForIndexes(context, dataset, indexes, fieldVarsForBeforeOperation, recType,
                    metaType, primaryIndexModificationOp.getBeforeOpRecordVar(), beforeOpMetaVar, currentTop, true);
        }

        // Iterate each secondary index and applying Index Update operations.
        // At first, op1 is the index insert op insertOp
        for (Index index : indexes) {
            if (!index.isSecondaryIndex()) {
                continue;
            }
            hasSecondaryIndex = true;
            // Get the secondary fields names and types
            List<List<String>> secondaryKeyFields = index.getKeyFieldNames();
            List<IAType> secondaryKeyTypes = index.getKeyFieldTypes();
            List<LogicalVariable> secondaryKeyVars = new ArrayList<>();
            List<Mutable<ILogicalExpression>> secondaryExpressions = new ArrayList<>();
            List<Mutable<ILogicalExpression>> beforeOpSecondaryExpressions = new ArrayList<>();
            ILogicalOperator replicateOutput;

            for (int i = 0; i < secondaryKeyFields.size(); i++) {
                IndexFieldId indexFieldId = new IndexFieldId(index.getKeyFieldSourceIndicators().get(i),
                        secondaryKeyFields.get(i), secondaryKeyTypes.get(i).getTypeTag());
                LogicalVariable skVar = fieldVarsForNewRecord.get(indexFieldId);
                secondaryKeyVars.add(skVar);
                VariableReferenceExpression skVarRef = new VariableReferenceExpression(skVar);
                skVarRef.setSourceLocation(sourceLoc);
                secondaryExpressions.add(new MutableObject<ILogicalExpression>(skVarRef));
                if (primaryIndexModificationOp.getOperation() == Kind.UPSERT) {
                    VariableReferenceExpression varRef =
                            new VariableReferenceExpression(fieldVarsForBeforeOperation.get(indexFieldId));
                    varRef.setSourceLocation(sourceLoc);
                    beforeOpSecondaryExpressions.add(new MutableObject<ILogicalExpression>(varRef));
                }
            }

            IndexInsertDeleteUpsertOperator indexUpdate;
            if (index.getIndexType() != IndexType.RTREE) {
                // Create an expression per key
                Mutable<ILogicalExpression> filterExpression =
                        (primaryIndexModificationOp.getOperation() == Kind.UPSERT) ? null
                                : createFilterExpression(secondaryKeyVars, context.getOutputTypeEnvironment(currentTop),
                                        index.isOverridingKeyFieldTypes(), sourceLoc);
                DataSourceIndex dataSourceIndex = new DataSourceIndex(index, dataverseName, datasetName, mp);

                // Introduce the TokenizeOperator only when doing bulk-load,
                // and index type is keyword or n-gram.
                if (index.getIndexType() != IndexType.BTREE && primaryIndexModificationOp.isBulkload()) {
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
                    Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(
                            index.getKeyFieldTypes().get(0), secondaryKeyFields.get(0), recType);
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
                            primaryIndexModificationOp.getPrimaryKeyExpressions(), secondaryExpressions,
                            tokenizeKeyVars, filterExpression, primaryIndexModificationOp.getOperation(),
                            primaryIndexModificationOp.isBulkload(), isPartitioned, varTypes);
                    tokenUpdate.setSourceLocation(sourceLoc);
                    tokenUpdate.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
                    context.computeAndSetTypeEnvironmentForOperator(tokenUpdate);
                    replicateOutput = tokenUpdate;
                    indexUpdate = new IndexInsertDeleteUpsertOperator(dataSourceIndex,
                            primaryIndexModificationOp.getPrimaryKeyExpressions(), tokenizeKeyExprs, filterExpression,
                            primaryIndexModificationOp.getOperation(), primaryIndexModificationOp.isBulkload(),
                            primaryIndexModificationOp.getAdditionalNonFilteringExpressions() == null ? 0
                                    : primaryIndexModificationOp.getAdditionalNonFilteringExpressions().size());
                    indexUpdate.setSourceLocation(sourceLoc);
                    indexUpdate.setAdditionalFilteringExpressions(filteringExpressions);
                    indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(tokenUpdate));
                } else {
                    // When TokenizeOperator is not needed
                    indexUpdate = new IndexInsertDeleteUpsertOperator(dataSourceIndex,
                            primaryIndexModificationOp.getPrimaryKeyExpressions(), secondaryExpressions,
                            filterExpression, primaryIndexModificationOp.getOperation(),
                            primaryIndexModificationOp.isBulkload(),
                            primaryIndexModificationOp.getAdditionalNonFilteringExpressions() == null ? 0
                                    : primaryIndexModificationOp.getAdditionalNonFilteringExpressions().size());
                    indexUpdate.setSourceLocation(sourceLoc);
                    indexUpdate.setAdditionalFilteringExpressions(filteringExpressions);
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
                }
            } else {
                // Get type, dimensions and number of keys
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(0),
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
                Mutable<ILogicalExpression> filterExpression = null;
                AssignOperator originalAssignCoordinates = null;
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
                        createMBR.getArguments().add(beforeOpSecondaryExpressions.get(0));
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
                } else {
                    // We must enforce the filter if the originating spatial type is
                    // nullable.
                    boolean forceFilter = keyPairType.second;
                    filterExpression = createFilterExpression(keyVarList,
                            context.getOutputTypeEnvironment(assignCoordinates), forceFilter, sourceLoc);
                }
                DataSourceIndex dataSourceIndex = new DataSourceIndex(index, dataverseName, datasetName, mp);
                indexUpdate = new IndexInsertDeleteUpsertOperator(dataSourceIndex,
                        primaryIndexModificationOp.getPrimaryKeyExpressions(), secondaryExpressions, filterExpression,
                        primaryIndexModificationOp.getOperation(), primaryIndexModificationOp.isBulkload(),
                        primaryIndexModificationOp.getAdditionalNonFilteringExpressions() == null ? 0
                                : primaryIndexModificationOp.getAdditionalNonFilteringExpressions().size());
                indexUpdate.setSourceLocation(sourceLoc);
                indexUpdate.setAdditionalFilteringExpressions(filteringExpressions);
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
                indexUpdate.setUpsertIndicatorExpr(new MutableObject<>(
                        new VariableReferenceExpression(primaryIndexModificationOp.getUpsertIndicatorVar())));
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
                if (index.getKeyFieldNames().isEmpty() && index.getIndexType() == IndexType.BTREE) {
                    int positionOfSecondaryPrimaryIndex = replicateOp.getOutputs().size() - 1;
                    replicateOp.getOutputMaterializationFlags()[positionOfSecondaryPrimaryIndex] = true;
                }
            }
            if (primaryIndexModificationOp.isBulkload()) {
                // For bulk load, we connect all fanned out insert operator to a single SINK operator
                op0.getInputs().add(new MutableObject<ILogicalOperator>(indexUpdate));
            }

        }
        if (!hasSecondaryIndex) {
            return false;
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

    private LogicalVariable getRecordVar(IOptimizationContext context, AbstractLogicalOperator inputOp,
            ILogicalExpression recordExpr, int expectedRecordIndex) throws AlgebricksException {
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

    private ILogicalOperator injectFieldAccessesForIndexes(IOptimizationContext context, Dataset dataset,
            List<Index> indexes, Map<IndexFieldId, LogicalVariable> fieldAccessVars, ARecordType recType,
            ARecordType metaType, LogicalVariable recordVar, LogicalVariable metaVar, ILogicalOperator currentTop,
            boolean afterOp) throws AlgebricksException {
        List<LogicalVariable> vars = new ArrayList<>();
        List<Mutable<ILogicalExpression>> exprs = new ArrayList<>();
        SourceLocation sourceLoc = currentTop.getSourceLocation();
        for (Index index : indexes) {
            if (index.isPrimaryIndex()) {
                continue;
            }
            List<IAType> skTypes = index.getKeyFieldTypes();
            List<List<String>> skNames = index.getKeyFieldNames();
            List<Integer> indicators = index.getKeyFieldSourceIndicators();
            for (int i = 0; i < index.getKeyFieldNames().size(); i++) {
                IndexFieldId indexFieldId =
                        new IndexFieldId(indicators.get(i), skNames.get(i), skTypes.get(i).getTypeTag());
                if (fieldAccessVars.containsKey(indexFieldId)) {
                    // already handled in a different index
                    continue;
                }
                ARecordType sourceType = dataset.hasMetaPart()
                        ? indicators.get(i).intValue() == Index.RECORD_INDICATOR ? recType : metaType : recType;
                LogicalVariable sourceVar = dataset.hasMetaPart()
                        ? indicators.get(i).intValue() == Index.RECORD_INDICATOR ? recordVar : metaVar : recordVar;
                LogicalVariable fieldVar = context.newVar();
                // create record variable ref
                VariableReferenceExpression varRef = new VariableReferenceExpression(sourceVar);
                varRef.setSourceLocation(sourceLoc);
                IAType fieldType = sourceType.getSubFieldType(indexFieldId.fieldName);
                AbstractFunctionCallExpression theFieldAccessFunc;
                if (fieldType == null) {
                    // Open field. must prevent inlining to maintain the cast before the primaryOp and
                    // make handling of records with incorrect value type for this field easier and cleaner
                    context.addNotToBeInlinedVar(fieldVar);
                    // create field access
                    AbstractFunctionCallExpression fieldAccessFunc = getOpenOrNestedFieldAccessFunction(
                            new MutableObject<>(varRef), indexFieldId.fieldName, sourceLoc);
                    // create cast
                    theFieldAccessFunc = new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(
                            index.isEnforced() ? BuiltinFunctions.CAST_TYPE : BuiltinFunctions.CAST_TYPE_LAX));
                    theFieldAccessFunc.setSourceLocation(sourceLoc);
                    // The first argument is the field
                    theFieldAccessFunc.getArguments().add(new MutableObject<ILogicalExpression>(fieldAccessFunc));
                    TypeCastUtils.setRequiredAndInputTypes(theFieldAccessFunc, skTypes.get(i), BuiltinType.ANY);
                } else {
                    // Get the desired field position
                    int pos = indexFieldId.fieldName.size() > 1 ? -1
                            : sourceType.getFieldIndex(indexFieldId.fieldName.get(0));
                    // Field not found --> This is either an open field or a nested field. it can't be accessed by index
                    theFieldAccessFunc = (pos == -1)
                            ? getOpenOrNestedFieldAccessFunction(new MutableObject<>(varRef), indexFieldId.fieldName,
                                    sourceLoc)
                            : getClosedFieldAccessFunction(new MutableObject<>(varRef), pos, sourceLoc);
                }
                vars.add(fieldVar);
                exprs.add(new MutableObject<ILogicalExpression>(theFieldAccessFunc));
                fieldAccessVars.put(indexFieldId, fieldVar);
            }
        }
        // AssignOperator assigns secondary keys to their vars
        AssignOperator castedFieldAssignOperator = new AssignOperator(vars, exprs);
        castedFieldAssignOperator.setSourceLocation(sourceLoc);
        return introduceNewOp(context, currentTop, castedFieldAssignOperator, afterOp);
    }

    private static ILogicalOperator introduceNewOp(IOptimizationContext context, ILogicalOperator currentTopOp,
            ILogicalOperator newOp, boolean afterOp) throws AlgebricksException {
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

    private static AbstractFunctionCallExpression getClosedFieldAccessFunction(Mutable<ILogicalExpression> varRef,
            int position, SourceLocation sourceLoc) {
        Mutable<ILogicalExpression> indexRef =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(position))));
        ScalarFunctionCallExpression fnExpr = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_INDEX), varRef, indexRef);
        fnExpr.setSourceLocation(sourceLoc);
        return fnExpr;
    }

    private static AbstractFunctionCallExpression getOpenOrNestedFieldAccessFunction(Mutable<ILogicalExpression> varRef,
            List<String> fields, SourceLocation sourceLoc) {
        ScalarFunctionCallExpression func;
        if (fields.size() > 1) {
            IAObject fieldList = stringListToAOrderedList(fields);
            Mutable<ILogicalExpression> fieldRef = constantToMutableLogicalExpression(fieldList);
            // Create an expression for the nested case
            func = new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_NESTED),
                    varRef, fieldRef);
        } else {
            IAObject fieldList = new AString(fields.get(0));
            Mutable<ILogicalExpression> fieldRef = constantToMutableLogicalExpression(fieldList);
            // Create an expression for the open field case (By name)
            func = new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME),
                    varRef, fieldRef);
        }
        func.setSourceLocation(sourceLoc);
        return func;
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

    private Mutable<ILogicalExpression> createFilterExpression(List<LogicalVariable> secondaryKeyVars,
            IVariableTypeEnvironment typeEnv, boolean forceFilter, SourceLocation sourceLoc)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> filterExpressions = new ArrayList<>();
        // Add 'is not null' to all nullable secondary index keys as a filtering
        // condition.
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
            // Create a conjunctive condition.
            ScalarFunctionCallExpression andExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.AND), filterExpressions);
            andExpr.setSourceLocation(sourceLoc);
            filterExpression = new MutableObject<>(andExpr);
        } else {
            filterExpression = filterExpressions.get(0);
        }
        return filterExpression;
    }

    private final class IndexFieldId {
        private final int indicator;
        private final List<String> fieldName;
        private final ATypeTag fieldType;

        private IndexFieldId(int indicator, List<String> fieldName, ATypeTag fieldType) {
            this.indicator = indicator;
            this.fieldName = fieldName;
            this.fieldType = fieldType;
        }

        @Override
        public int hashCode() {
            int result = indicator;
            result = 31 * result + fieldName.hashCode();
            result = 31 * result + fieldType.hashCode();
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
            if (indicator != that.indicator) {
                return false;
            }
            if (!fieldName.equals(that.fieldName)) {
                return false;
            }
            return fieldType == that.fieldType;
        }
    }
}
