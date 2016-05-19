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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Stack;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.AqlDataSource;
import org.apache.asterix.metadata.declared.AqlIndex;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.TypeComputerUtilities;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.commons.lang3.ArrayUtils;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator.Kind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

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
        if (op0.getOperatorTag() != LogicalOperatorTag.SINK) {
            return false;
        }
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) op0.getInputs().get(0).getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE_UPSERT) {
            return false;
        }

        FunctionIdentifier fid = null;
        /** find the record variable */
        InsertDeleteUpsertOperator insertOp = (InsertDeleteUpsertOperator) op1;
        boolean isBulkload = insertOp.isBulkload();
        ILogicalExpression recordExpr = insertOp.getPayloadExpression().getValue();
        LogicalVariable recordVar = null;
        List<LogicalVariable> usedRecordVars = new ArrayList<>();
        /** assume the payload is always a single variable expression */
        recordExpr.getUsedVariables(usedRecordVars);
        if (usedRecordVars.size() == 1) {
            recordVar = usedRecordVars.get(0);
        }

        /**
         * op2 is the assign operator which extract primary keys from the record
         * variable
         */
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();

        if (recordVar == null) {
            /**
             * For the case primary key-assignment expressions are constant
             * expressions, find assign op that creates record to be
             * inserted/deleted.
             */
            while (fid != AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR) {
                if (op2.getInputs().size() == 0) {
                    return false;
                }
                op2 = (AbstractLogicalOperator) op2.getInputs().get(0).getValue();
                if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
                    continue;
                }
                AssignOperator assignOp = (AssignOperator) op2;
                ILogicalExpression assignExpr = assignOp.getExpressions().get(0).getValue();
                if (assignExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    ScalarFunctionCallExpression funcExpr = (ScalarFunctionCallExpression) assignOp.getExpressions()
                            .get(0).getValue();
                    fid = funcExpr.getFunctionIdentifier();
                }
            }
            AssignOperator assignOp2 = (AssignOperator) op2;
            recordVar = assignOp2.getVariables().get(0);
        }

        /*
         * At this point, we have the record variable and the insert/delete/upsert operator
         * Note: We have two operators:
         * 1. An InsertDeleteOperator (primary)
         * 2. An IndexInsertDeleteOperator (secondary)
         * The current insertOp is of the first type
         */

        AqlDataSource datasetSource = (AqlDataSource) insertOp.getDataSource();
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        String dataverseName = datasetSource.getId().getDataverseName();
        String datasetName = datasetSource.getId().getDatasourceName();
        Dataset dataset = mp.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return false;
        }

        // Create operators for secondary index insert/delete.
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType = mp.findType(dataset.getItemTypeDataverseName(), itemTypeName);
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Only record types can be indexed.");
        }
        ARecordType recType = (ARecordType) itemType;
        // recType might be replaced with enforced record type and we want to keep a reference to the original record
        // type
        ARecordType originalRecType = recType;
        List<Index> indexes = mp.getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName());
        // Set the top operator pointer to the primary IndexInsertDeleteOperator
        ILogicalOperator currentTop = op1;
        boolean hasSecondaryIndex = false;

        // Put an n-gram or a keyword index in the later stage of index-update,
        // since TokenizeOperator needs to be involved.
        Collections.sort(indexes, new Comparator<Index>() {
            @Override
            public int compare(Index o1, Index o2) {
                return o1.getIndexType().ordinal() - o2.getIndexType().ordinal();
            }

        });

        // Check whether multiple indexes exist
        int secondaryIndexTotalCnt = 0;
        for (Index index : indexes) {
            if (index.isSecondaryIndex()) {
                secondaryIndexTotalCnt++;
            }
        }

        // At this point, we have the data type info, and the indexes info as well
        // Initialize inputs to the SINK operator Op0 (The SINK) is now without input
        if (secondaryIndexTotalCnt > 0) {
            op0.getInputs().clear();
        }

        // Prepare filtering field information (This is the filter created using the "filter with" key word in the
        // create dataset ddl)
        List<String> filteringFields = ((InternalDatasetDetails) dataset.getDatasetDetails()).getFilterField();
        List<LogicalVariable> filteringVars = null;
        List<Mutable<ILogicalExpression>> filteringExpressions = null;

        if (filteringFields != null) {
            // The filter field var already exists. we can simply get it from the insert op
            filteringVars = new ArrayList<LogicalVariable>();
            filteringExpressions = new ArrayList<Mutable<ILogicalExpression>>();
            for (Mutable<ILogicalExpression> filteringExpression : insertOp.getAdditionalFilteringExpressions()) {
                filteringExpression.getValue().getUsedVariables(filteringVars);
                for (LogicalVariable var : filteringVars) {
                    filteringExpressions
                            .add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(var)));
                }
            }
        }
        LogicalVariable enforcedRecordVar = recordVar;

        /*
         * if the index is enforcing field types (For open indexes), We add a cast
         * operator to ensure type safety
         */
        if (insertOp.getOperation() == Kind.INSERT || insertOp.getOperation() == Kind.UPSERT) {
            try {
                DatasetDataSource ds = (DatasetDataSource) (insertOp.getDataSource());
                ARecordType insertRecType = (ARecordType) ds.getSchemaTypes()[ds.getSchemaTypes().length - 1];
                // A new variable which represents the casted record
                LogicalVariable castedRecVar = context.newVar();
                // create the expected record type = the original + the optional open field
                ARecordType enforcedType = createEnforcedType(insertRecType, indexes);
                if (!enforcedType.equals(insertRecType)) {
                    //introduce casting to enforced type
                    AbstractFunctionCallExpression castFunc = new ScalarFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.CAST_RECORD));
                    // The first argument is the record
                    castFunc.getArguments()
                            .add(new MutableObject<ILogicalExpression>(insertOp.getPayloadExpression().getValue()));
                    TypeComputerUtilities.setRequiredAndInputTypes(castFunc, enforcedType, insertRecType);
                    // AssignOperator puts in the cast var the casted record
                    AssignOperator castedRecordAssignOperator = new AssignOperator(castedRecVar,
                            new MutableObject<ILogicalExpression>(castFunc));
                    // Connect the current top of the plan to the cast operator
                    castedRecordAssignOperator.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
                    currentTop = castedRecordAssignOperator;
                    enforcedRecordVar = castedRecVar;
                    recType = enforcedType;
                    context.computeAndSetTypeEnvironmentForOperator(castedRecordAssignOperator);
                    // We don't need to cast the old rec, we just need an assignment function that extracts the SK
                    // and an expression which reference the new variables.
                }
            } catch (AsterixException e) {
                throw new AlgebricksException(e);
            }
        }

        // Replicate Operator is applied only when doing the bulk-load.
        AbstractLogicalOperator replicateOp = null;
        if (secondaryIndexTotalCnt > 1 && insertOp.isBulkload()) {
            // Split the logical plan into "each secondary index update branch"
            // to replicate each <PK,RECORD> pair.
            replicateOp = new ReplicateOperator(secondaryIndexTotalCnt);
            replicateOp.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
            replicateOp.setExecutionMode(ExecutionMode.PARTITIONED);
            context.computeAndSetTypeEnvironmentForOperator(replicateOp);
            currentTop = replicateOp;
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
            List<LogicalVariable> secondaryKeyVars = new ArrayList<LogicalVariable>();
            List<Mutable<ILogicalExpression>> expressions = new ArrayList<Mutable<ILogicalExpression>>();
            List<Mutable<ILogicalExpression>> secondaryExpressions = new ArrayList<Mutable<ILogicalExpression>>();

            for (List<String> secondaryKey : secondaryKeyFields) {
                prepareVarAndExpression(secondaryKey, recType.getFieldNames(), enforcedRecordVar, expressions,
                        secondaryKeyVars, context);
            }
            // Used with upsert operation
            // in case of upsert, we need vars and expressions for the old SK as well.
            List<LogicalVariable> prevSecondaryKeyVars = null;
            List<Mutable<ILogicalExpression>> prevExpressions = null;
            List<Mutable<ILogicalExpression>> prevSecondaryExpressions = null;
            AssignOperator prevSecondaryKeyAssign = null;
            if (insertOp.getOperation() == Kind.UPSERT) {
                prevSecondaryKeyVars = new ArrayList<LogicalVariable>();
                prevExpressions = new ArrayList<Mutable<ILogicalExpression>>();
                prevSecondaryExpressions = new ArrayList<Mutable<ILogicalExpression>>();
                for (List<String> secondaryKey : secondaryKeyFields) {
                    prepareVarAndExpression(secondaryKey, originalRecType.getFieldNames(), insertOp.getPrevRecordVar(),
                            prevExpressions, prevSecondaryKeyVars, context);
                }
                prevSecondaryKeyAssign = new AssignOperator(prevSecondaryKeyVars, prevExpressions);
            }
            AssignOperator assign = new AssignOperator(secondaryKeyVars, expressions);

            AssignOperator topAssign = assign;
            if (insertOp.getOperation() == Kind.UPSERT) {
                prevSecondaryKeyAssign.getInputs().add(new MutableObject<ILogicalOperator>(topAssign));
                topAssign = prevSecondaryKeyAssign;
            }
            // Only apply replicate operator when doing bulk-load
            if (secondaryIndexTotalCnt > 1 && insertOp.isBulkload()) {
                assign.getInputs().add(new MutableObject<ILogicalOperator>(replicateOp));
            } else {
                assign.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
            }

            context.computeAndSetTypeEnvironmentForOperator(assign);
            if (insertOp.getOperation() == Kind.UPSERT) {
                context.computeAndSetTypeEnvironmentForOperator(prevSecondaryKeyAssign);
            }
            currentTop = topAssign;

            // in case of an Upsert operation, the currentTop is an assign which has the old secondary keys + the new secondary keys
            if (index.getIndexType() == IndexType.BTREE || index.getIndexType() == IndexType.SINGLE_PARTITION_WORD_INVIX
                    || index.getIndexType() == IndexType.SINGLE_PARTITION_NGRAM_INVIX
                    || index.getIndexType() == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                    || index.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
                // Create an expression per key
                for (LogicalVariable secondaryKeyVar : secondaryKeyVars) {
                    secondaryExpressions.add(
                            new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVar)));
                }
                Mutable<ILogicalExpression> filterExpression = null;
                if (insertOp.getOperation() == Kind.UPSERT) {
                    for (LogicalVariable oldSecondaryKeyVar : prevSecondaryKeyVars) {
                        prevSecondaryExpressions.add(new MutableObject<ILogicalExpression>(
                                new VariableReferenceExpression(oldSecondaryKeyVar)));
                    }
                } else {
                    filterExpression = createFilterExpression(secondaryKeyVars,
                            context.getOutputTypeEnvironment(currentTop), false);
                }
                AqlIndex dataSourceIndex = new AqlIndex(index, dataverseName, datasetName, mp);

                // Introduce the TokenizeOperator only when doing bulk-load,
                // and index type is keyword or n-gram.
                if (index.getIndexType() != IndexType.BTREE && insertOp.isBulkload()) {
                    // Note: Bulk load case, we don't need to take care of it for upsert operation
                    // Check whether the index is length-partitioned or not.
                    // If partitioned, [input variables to TokenizeOperator,
                    // token, number of token] pairs will be generated and
                    // fed into the IndexInsertDeleteOperator.
                    // If not, [input variables, token] pairs will be generated
                    // and fed into the IndexInsertDeleteOperator.
                    // Input variables are passed since TokenizeOperator is not an
                    // filtering operator.
                    boolean isPartitioned = false;
                    if (index.getIndexType() == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                            || index.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
                        isPartitioned = true;
                    }

                    // Create a new logical variable - token
                    List<LogicalVariable> tokenizeKeyVars = new ArrayList<LogicalVariable>();
                    List<Mutable<ILogicalExpression>> tokenizeKeyExprs = new ArrayList<Mutable<ILogicalExpression>>();
                    LogicalVariable tokenVar = context.newVar();
                    tokenizeKeyVars.add(tokenVar);
                    tokenizeKeyExprs
                            .add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(tokenVar)));

                    // Check the field type of the secondary key.
                    IAType secondaryKeyType = null;
                    Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(secondaryKeyFields.get(0),
                            recType);
                    secondaryKeyType = keyPairType.first;

                    List<Object> varTypes = new ArrayList<Object>();
                    varTypes.add(NonTaggedFormatUtil.getTokenType(secondaryKeyType));

                    // If the index is a length-partitioned, then create
                    // additional variable - number of token.
                    // We use a special type for the length-partitioned index.
                    // The type is short, and this does not contain type info.
                    if (isPartitioned) {
                        LogicalVariable lengthVar = context.newVar();
                        tokenizeKeyVars.add(lengthVar);
                        tokenizeKeyExprs
                                .add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(lengthVar)));
                        varTypes.add(BuiltinType.SHORTWITHOUTTYPEINFO);
                    }

                    // TokenizeOperator to tokenize [SK, PK] pairs
                    TokenizeOperator tokenUpdate = new TokenizeOperator(dataSourceIndex,
                            insertOp.getPrimaryKeyExpressions(), secondaryExpressions, tokenizeKeyVars,
                            filterExpression, insertOp.getOperation(), insertOp.isBulkload(), isPartitioned, varTypes);
                    tokenUpdate.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                    context.computeAndSetTypeEnvironmentForOperator(tokenUpdate);

                    IndexInsertDeleteUpsertOperator indexUpdate = new IndexInsertDeleteUpsertOperator(dataSourceIndex,
                            insertOp.getPrimaryKeyExpressions(), tokenizeKeyExprs, filterExpression,
                            insertOp.getOperation(), insertOp.isBulkload());
                    indexUpdate.setAdditionalFilteringExpressions(filteringExpressions);
                    indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(tokenUpdate));

                    context.computeAndSetTypeEnvironmentForOperator(indexUpdate);

                    currentTop = indexUpdate;
                    op0.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));

                } else {
                    // When TokenizeOperator is not needed
                    IndexInsertDeleteUpsertOperator indexUpdate = new IndexInsertDeleteUpsertOperator(dataSourceIndex,
                            insertOp.getPrimaryKeyExpressions(), secondaryExpressions, filterExpression,
                            insertOp.getOperation(), insertOp.isBulkload());

                    indexUpdate.setAdditionalFilteringExpressions(filteringExpressions);
                    // We add the necessary expressions for upsert
                    if (insertOp.getOperation() == Kind.UPSERT) {
                        indexUpdate.setPrevSecondaryKeyExprs(prevSecondaryExpressions);
                        if (filteringFields != null) {
                            indexUpdate.setPrevAdditionalFilteringExpression(new MutableObject<ILogicalExpression>(
                                    new VariableReferenceExpression(insertOp.getPrevFilterVar())));
                        }
                    }
                    indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));

                    currentTop = indexUpdate;
                    context.computeAndSetTypeEnvironmentForOperator(indexUpdate);

                    if (insertOp.isBulkload()) {
                        op0.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
                    }

                }

            } else if (index.getIndexType() == IndexType.RTREE) {
                // Get type, dimensions and number of keys
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(0),
                        secondaryKeyFields.get(0), recType);
                IAType spatialType = keyPairType.first;
                boolean isPointMBR = spatialType.getTypeTag() == ATypeTag.POINT
                        || spatialType.getTypeTag() == ATypeTag.POINT3D;
                int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
                int numKeys = (isPointMBR && isBulkload) ? dimension : dimension * 2;
                // Get variables and expressions
                List<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
                List<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
                for (int i = 0; i < numKeys; i++) {
                    LogicalVariable keyVar = context.newVar();
                    keyVarList.add(keyVar);
                    AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.CREATE_MBR));
                    createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                            new VariableReferenceExpression(secondaryKeyVars.get(0))));
                    createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                            new ConstantExpression(new AsterixConstantValue(new AInt32(dimension)))));
                    createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                            new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
                    keyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
                }
                for (LogicalVariable secondaryKeyVar : keyVarList) {
                    secondaryExpressions.add(
                            new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVar)));
                }
                if (isPointMBR && isBulkload) {
                    //for PointMBR optimization: see SecondaryRTreeOperationsHelper.buildLoadingJobSpec() and 
                    //createFieldPermutationForBulkLoadOp(int) for more details.
                    for (LogicalVariable secondaryKeyVar : keyVarList) {
                        secondaryExpressions.add(new MutableObject<ILogicalExpression>(
                                new VariableReferenceExpression(secondaryKeyVar)));
                    }
                }
                AssignOperator assignCoordinates = new AssignOperator(keyVarList, keyExprList);
                assignCoordinates.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
                context.computeAndSetTypeEnvironmentForOperator(assignCoordinates);
                Mutable<ILogicalExpression> filterExpression = null;
                AssignOperator originalAssignCoordinates = null;
                // We do something similar for previous key if the operation is an upsert
                if (insertOp.getOperation() == Kind.UPSERT) {
                    List<LogicalVariable> originalKeyVarList = new ArrayList<LogicalVariable>();
                    List<Mutable<ILogicalExpression>> originalKeyExprList = new ArrayList<Mutable<ILogicalExpression>>();
                    // we don't do any filtering since nulls are expected here and there
                    for (int i = 0; i < numKeys; i++) {
                        LogicalVariable keyVar = context.newVar();
                        originalKeyVarList.add(keyVar);
                        AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                                FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.CREATE_MBR));
                        createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                                new VariableReferenceExpression(prevSecondaryKeyVars.get(0))));
                        createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                                new ConstantExpression(new AsterixConstantValue(new AInt32(dimension)))));
                        createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                                new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
                        originalKeyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
                    }
                    for (LogicalVariable secondaryKeyVar : originalKeyVarList) {
                        prevSecondaryExpressions.add(new MutableObject<ILogicalExpression>(
                                new VariableReferenceExpression(secondaryKeyVar)));
                    }
                    if (isPointMBR && isBulkload) {
                        //for PointMBR optimization: see SecondaryRTreeOperationsHelper.buildLoadingJobSpec() and 
                        //createFieldPermutationForBulkLoadOp(int) for more details.
                        for (LogicalVariable secondaryKeyVar : originalKeyVarList) {
                            prevSecondaryExpressions.add(new MutableObject<ILogicalExpression>(
                                    new VariableReferenceExpression(secondaryKeyVar)));
                        }
                    }
                    originalAssignCoordinates = new AssignOperator(originalKeyVarList, originalKeyExprList);
                    originalAssignCoordinates.getInputs().add(new MutableObject<ILogicalOperator>(assignCoordinates));
                    context.computeAndSetTypeEnvironmentForOperator(originalAssignCoordinates);
                } else {
                    // We must enforce the filter if the originating spatial type is
                    // nullable.
                    boolean forceFilter = keyPairType.second;
                    filterExpression = createFilterExpression(keyVarList,
                            context.getOutputTypeEnvironment(assignCoordinates), forceFilter);
                }
                AqlIndex dataSourceIndex = new AqlIndex(index, dataverseName, datasetName, mp);
                IndexInsertDeleteUpsertOperator indexUpdate = new IndexInsertDeleteUpsertOperator(dataSourceIndex,
                        insertOp.getPrimaryKeyExpressions(), secondaryExpressions, filterExpression,
                        insertOp.getOperation(), insertOp.isBulkload());
                indexUpdate.setAdditionalFilteringExpressions(filteringExpressions);
                if (insertOp.getOperation() == Kind.UPSERT) {
                    // set old secondary key expressions
                    if (filteringFields != null) {
                        indexUpdate.setPrevAdditionalFilteringExpression(new MutableObject<ILogicalExpression>(
                                new VariableReferenceExpression(insertOp.getPrevFilterVar())));
                    }
                    // set filtering expressions
                    indexUpdate.setPrevSecondaryKeyExprs(prevSecondaryExpressions);
                    // assign --> assign previous values --> secondary index upsert
                    indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(originalAssignCoordinates));
                } else {
                    indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(assignCoordinates));
                }
                currentTop = indexUpdate;
                context.computeAndSetTypeEnvironmentForOperator(indexUpdate);

                if (insertOp.isBulkload()) {
                    // For bulk load, we connect all fanned out insert operator to a single SINK operator
                    op0.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
                }

            }

        }
        if (!hasSecondaryIndex) {
            return false;
        }

        if (!insertOp.isBulkload()) {
            // If this is an upsert, we need to
            // Remove the current input to the SINK operator (It is actually already removed above)
            op0.getInputs().clear();
            // Connect the last index update to the SINK
            op0.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
        }
        return true;
    }

    // Merges typed index fields with specified recordType, allowing indexed fields to be optional.
    // I.e. the type { "personId":int32, "name": string, "address" : { "street": string } } with typed indexes on age:int32, address.state:string
    //      will be merged into type { "personId":int32, "name": string, "age": int32? "address" : { "street": string, "state": string? } }
    // Used by open indexes to enforce the type of an indexed record
    public static ARecordType createEnforcedType(ARecordType initialType, List<Index> indexes)
            throws AsterixException, AlgebricksException {
        ARecordType enforcedType = initialType;
        for (Index index : indexes) {
            if (!index.isSecondaryIndex() || !index.isEnforcingKeyFileds()) {
                continue;
            }
            for (int i = 0; i < index.getKeyFieldNames().size(); i++) {
                Stack<Pair<ARecordType, String>> nestedTypeStack = new Stack<Pair<ARecordType, String>>();
                List<String> splits = index.getKeyFieldNames().get(i);
                ARecordType nestedFieldType = enforcedType;
                boolean openRecords = false;
                String bridgeName = nestedFieldType.getTypeName();
                int j;
                // Build the stack for the enforced type
                for (j = 1; j < splits.size(); j++) {
                    nestedTypeStack.push(new Pair<ARecordType, String>(nestedFieldType, splits.get(j - 1)));
                    bridgeName = nestedFieldType.getTypeName();
                    nestedFieldType = (ARecordType) enforcedType.getSubFieldType(splits.subList(0, j));
                    if (nestedFieldType == null) {
                        openRecords = true;
                        break;
                    }
                }
                if (openRecords == true) {
                    // create the smallest record
                    enforcedType = new ARecordType(splits.get(splits.size() - 2),
                            new String[] { splits.get(splits.size() - 1) },
                            new IAType[] { AUnionType.createNullableType(index.getKeyFieldTypes().get(i)) }, true);
                    // create the open part of the nested field
                    for (int k = splits.size() - 3; k > (j - 2); k--) {
                        enforcedType = new ARecordType(splits.get(k), new String[] { splits.get(k + 1) },
                                new IAType[] { AUnionType.createNullableType(enforcedType) }, true);
                    }
                    // Bridge the gap
                    Pair<ARecordType, String> gapPair = nestedTypeStack.pop();
                    ARecordType parent = gapPair.first;

                    IAType[] parentFieldTypes = ArrayUtils.addAll(parent.getFieldTypes().clone(),
                            new IAType[] { AUnionType.createNullableType(enforcedType) });
                    enforcedType = new ARecordType(bridgeName,
                            ArrayUtils.addAll(parent.getFieldNames(), enforcedType.getTypeName()), parentFieldTypes,
                            true);

                } else {
                    //Schema is closed all the way to the field
                    //enforced fields are either null or strongly typed
                    LinkedHashMap<String, IAType> recordNameTypesMap = new LinkedHashMap<String, IAType>();
                    for (j = 0; j < nestedFieldType.getFieldNames().length; j++) {
                        recordNameTypesMap.put(nestedFieldType.getFieldNames()[j], nestedFieldType.getFieldTypes()[j]);
                    }
                    // if a an enforced field already exists and the type is correct
                    IAType enforcedFieldType = recordNameTypesMap.get(splits.get(splits.size() - 1));
                    if (enforcedFieldType != null && enforcedFieldType.getTypeTag() == ATypeTag.UNION
                            && ((AUnionType) enforcedFieldType).isNullableType()) {
                        enforcedFieldType = ((AUnionType) enforcedFieldType).getNullableType();
                    }
                    if (enforcedFieldType != null && !ATypeHierarchy.canPromote(enforcedFieldType.getTypeTag(),
                            index.getKeyFieldTypes().get(i).getTypeTag())) {
                        throw new AlgebricksException("Cannot enforce field " + index.getKeyFieldNames().get(i)
                                + " to have type " + index.getKeyFieldTypes().get(i));
                    }
                    if (enforcedFieldType == null) {
                        recordNameTypesMap.put(splits.get(splits.size() - 1),
                                AUnionType.createNullableType(index.getKeyFieldTypes().get(i)));
                    }
                    enforcedType = new ARecordType(nestedFieldType.getTypeName(),
                            recordNameTypesMap.keySet().toArray(new String[recordNameTypesMap.size()]),
                            recordNameTypesMap.values().toArray(new IAType[recordNameTypesMap.size()]),
                            nestedFieldType.isOpen());
                }

                // Create the enforcedtype for the nested fields in the schema, from the ground up
                if (nestedTypeStack.size() > 0) {
                    while (!nestedTypeStack.isEmpty()) {
                        Pair<ARecordType, String> nestedTypePair = nestedTypeStack.pop();
                        ARecordType nestedRecType = nestedTypePair.first;
                        IAType[] nestedRecTypeFieldTypes = nestedRecType.getFieldTypes().clone();
                        nestedRecTypeFieldTypes[nestedRecType.getFieldIndex(nestedTypePair.second)] = enforcedType;
                        enforcedType = new ARecordType(nestedRecType.getTypeName() + "_enforced",
                                nestedRecType.getFieldNames(), nestedRecTypeFieldTypes, nestedRecType.isOpen());
                    }
                }
            }
        }
        return enforcedType;
    }

    /***
     * This method takes a list of {fields}: a subset of {recordFields}, the original record variable
     * and populate expressions with expressions which evaluate to those fields (using field access functions) and
     * variables to represent them
     *
     * @param fields
     *            desired fields
     * @param recordFields
     *            all the record fields
     * @param recordVar
     *            the record variable
     * @param expressions
     * @param vars
     * @param context
     * @throws AlgebricksException
     */
    @SuppressWarnings("unchecked")
    private void prepareVarAndExpression(List<String> fields, String[] recordFields, LogicalVariable recordVar,
            List<Mutable<ILogicalExpression>> expressions, List<LogicalVariable> vars, IOptimizationContext context)
            throws AlgebricksException {
        // Get a reference to the record variable
        Mutable<ILogicalExpression> varRef = new MutableObject<ILogicalExpression>(
                new VariableReferenceExpression(recordVar));
        // Get the desired field position
        int pos = -1;
        if (fields.size() == 1) {
            for (int j = 0; j < recordFields.length; j++) {
                if (recordFields[j].equals(fields.get(0))) {
                    pos = j;
                    break;
                }
            }
        }
        // Field not found --> This is either an open field or a nested field. it can't be accessed by index
        AbstractFunctionCallExpression func;
        if (pos == -1) {
            if (fields.size() > 1) {
                AOrderedList fieldList = new AOrderedList(new AOrderedListType(BuiltinType.ASTRING, null));
                for (int i = 0; i < fields.size(); i++) {
                    fieldList.add(new AString(fields.get(i)));
                }
                Mutable<ILogicalExpression> fieldRef = new MutableObject<ILogicalExpression>(
                        new ConstantExpression(new AsterixConstantValue(fieldList)));
                // Create an expression for the nested case
                func = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_NESTED), varRef, fieldRef);
            } else {
                Mutable<ILogicalExpression> fieldRef = new MutableObject<ILogicalExpression>(
                        new ConstantExpression(new AsterixConstantValue(new AString(fields.get(0)))));
                // Create an expression for the open field case (By name)
                func = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME), varRef, fieldRef);
            }
        } else {
            // Assumes the indexed field is in the closed portion of the type.
            Mutable<ILogicalExpression> indexRef = new MutableObject<ILogicalExpression>(
                    new ConstantExpression(new AsterixConstantValue(new AInt32(pos))));
            func = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX), varRef, indexRef);
        }
        expressions.add(new MutableObject<ILogicalExpression>(func));
        LogicalVariable newVar = context.newVar();
        vars.add(newVar);
    }

    @SuppressWarnings("unchecked")
    private Mutable<ILogicalExpression> createFilterExpression(List<LogicalVariable> secondaryKeyVars,
            IVariableTypeEnvironment typeEnv, boolean forceFilter) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> filterExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        // Add 'is not null' to all nullable secondary index keys as a filtering
        // condition.
        for (LogicalVariable secondaryKeyVar : secondaryKeyVars) {
            IAType secondaryKeyType = (IAType) typeEnv.getVarType(secondaryKeyVar);
            if (!NonTaggedFormatUtil.isOptional(secondaryKeyType) && !forceFilter) {
                continue;
            }
            ScalarFunctionCallExpression isNullFuncExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.IS_NULL),
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVar)));
            ScalarFunctionCallExpression notFuncExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.NOT),
                    new MutableObject<ILogicalExpression>(isNullFuncExpr));
            filterExpressions.add(new MutableObject<ILogicalExpression>(notFuncExpr));
        }
        // No nullable secondary keys.
        if (filterExpressions.isEmpty()) {
            return null;
        }
        Mutable<ILogicalExpression> filterExpression = null;
        if (filterExpressions.size() > 1) {
            // Create a conjunctive condition.
            filterExpression = new MutableObject<ILogicalExpression>(new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.AND), filterExpressions));
        } else {
            filterExpression = filterExpressions.get(0);
        }
        return filterExpression;
    }
}
