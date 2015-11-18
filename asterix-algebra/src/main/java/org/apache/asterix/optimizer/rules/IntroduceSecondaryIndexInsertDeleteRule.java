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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
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
        if (op1.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE) {
            return false;
        }

        FunctionIdentifier fid = null;
        /** find the record variable */
        InsertDeleteOperator insertOp = (InsertDeleteOperator) op1;
        ILogicalExpression recordExpr = insertOp.getPayloadExpression().getValue();
        List<LogicalVariable> recordVar = new ArrayList<LogicalVariable>();
        /** assume the payload is always a single variable expression */
        recordExpr.getUsedVariables(recordVar);

        /**
         * op2 is the assign operator which extract primary keys from the record
         * variable
         */
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();

        if (recordVar.size() == 0) {
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
            recordVar.addAll(assignOp2.getVariables());
        }
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
        IAType itemType = mp.findType(dataset.getDataverseName(), itemTypeName);
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Only record types can be indexed.");
        }
        ARecordType recType = (ARecordType) itemType;
        List<Index> indexes = mp.getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName());
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

        // Check whether multiple keyword or n-gram indexes exist
        int secondaryIndexTotalCnt = 0;
        for (Index index : indexes) {
            if (index.isSecondaryIndex())
                secondaryIndexTotalCnt++;
        }

        // Initialize inputs to the SINK operator
        if (secondaryIndexTotalCnt > 0) {
            op0.getInputs().clear();
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

        // Prepare filtering field information
        List<String> additionalFilteringField = ((InternalDatasetDetails) dataset.getDatasetDetails()).getFilterField();
        List<LogicalVariable> additionalFilteringVars = null;
        List<Mutable<ILogicalExpression>> additionalFilteringAssignExpressions = null;
        List<Mutable<ILogicalExpression>> additionalFilteringExpressions = null;
        AssignOperator additionalFilteringAssign = null;

        if (additionalFilteringField != null) {
            additionalFilteringVars = new ArrayList<LogicalVariable>();
            additionalFilteringAssignExpressions = new ArrayList<Mutable<ILogicalExpression>>();
            additionalFilteringExpressions = new ArrayList<Mutable<ILogicalExpression>>();
            prepareVarAndExpression(additionalFilteringField, recType.getFieldNames(), recordVar.get(0),
                    additionalFilteringAssignExpressions, additionalFilteringVars, context);
            additionalFilteringAssign = new AssignOperator(additionalFilteringVars,
                    additionalFilteringAssignExpressions);
            for (LogicalVariable var : additionalFilteringVars) {
                additionalFilteringExpressions
                        .add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(var)));
            }
        }

        // Iterate each secondary index and applying Index Update operations.
        for (Index index : indexes) {
            List<LogicalVariable> projectVars = new ArrayList<LogicalVariable>();
            VariableUtilities.getUsedVariables(op1, projectVars);
            if (!index.isSecondaryIndex()) {
                continue;
            }
            LogicalVariable enforcedRecordVar = recordVar.get(0);
            hasSecondaryIndex = true;
            //if the index is enforcing field types
            if (index.isEnforcingKeyFileds()) {
                try {
                    DatasetDataSource ds = (DatasetDataSource) (insertOp.getDataSource());
                    ARecordType insertRecType = (ARecordType) ds.getSchemaTypes()[ds.getSchemaTypes().length - 1];
                    LogicalVariable castVar = context.newVar();
                    ARecordType enforcedType = createEnforcedType(insertRecType, index);
                    //introduce casting to enforced type
                    AbstractFunctionCallExpression castFunc = new ScalarFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.CAST_RECORD));

                    castFunc.getArguments()
                            .add(new MutableObject<ILogicalExpression>(insertOp.getPayloadExpression().getValue()));
                    TypeComputerUtilities.setRequiredAndInputTypes(castFunc, enforcedType, insertRecType);
                    AssignOperator newAssignOperator = new AssignOperator(castVar,
                            new MutableObject<ILogicalExpression>(castFunc));
                    newAssignOperator.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
                    currentTop = newAssignOperator;
                    //project out casted record
                    projectVars.add(castVar);
                    enforcedRecordVar = castVar;
                    context.computeAndSetTypeEnvironmentForOperator(newAssignOperator);
                    context.computeAndSetTypeEnvironmentForOperator(currentTop);
                    recType = enforcedType;
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }
            }

            List<List<String>> secondaryKeyFields = index.getKeyFieldNames();
            List<IAType> secondaryKeyTypes = index.getKeyFieldTypes();
            List<LogicalVariable> secondaryKeyVars = new ArrayList<LogicalVariable>();
            List<Mutable<ILogicalExpression>> expressions = new ArrayList<Mutable<ILogicalExpression>>();
            List<Mutable<ILogicalExpression>> secondaryExpressions = new ArrayList<Mutable<ILogicalExpression>>();

            for (List<String> secondaryKey : secondaryKeyFields) {
                prepareVarAndExpression(secondaryKey, recType.getFieldNames(), enforcedRecordVar, expressions,
                        secondaryKeyVars, context);
            }

            AssignOperator assign = new AssignOperator(secondaryKeyVars, expressions);
            ProjectOperator project = new ProjectOperator(projectVars);

            if (additionalFilteringAssign != null) {
                additionalFilteringAssign.getInputs().add(new MutableObject<ILogicalOperator>(project));
                assign.getInputs().add(new MutableObject<ILogicalOperator>(additionalFilteringAssign));
            } else {
                assign.getInputs().add(new MutableObject<ILogicalOperator>(project));
            }

            // Only apply replicate operator when doing bulk-load
            if (secondaryIndexTotalCnt > 1 && insertOp.isBulkload())
                project.getInputs().add(new MutableObject<ILogicalOperator>(replicateOp));
            else
                project.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));

            context.computeAndSetTypeEnvironmentForOperator(project);

            if (additionalFilteringAssign != null) {
                context.computeAndSetTypeEnvironmentForOperator(additionalFilteringAssign);
            }

            context.computeAndSetTypeEnvironmentForOperator(assign);
            currentTop = assign;

            // BTree, Keyword, or n-gram index case
            if (index.getIndexType() == IndexType.BTREE || index.getIndexType() == IndexType.SINGLE_PARTITION_WORD_INVIX
                    || index.getIndexType() == IndexType.SINGLE_PARTITION_NGRAM_INVIX
                    || index.getIndexType() == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                    || index.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
                for (LogicalVariable secondaryKeyVar : secondaryKeyVars) {
                    secondaryExpressions.add(
                            new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVar)));
                }
                Mutable<ILogicalExpression> filterExpression = createFilterExpression(secondaryKeyVars,
                        context.getOutputTypeEnvironment(currentTop), false);
                AqlIndex dataSourceIndex = new AqlIndex(index, dataverseName, datasetName, mp);

                // Introduce the TokenizeOperator only when doing bulk-load,
                // and index type is keyword or n-gram.
                if (index.getIndexType() != IndexType.BTREE && insertOp.isBulkload()) {

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
                            || index.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX)
                        isPartitioned = true;

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

                    IndexInsertDeleteOperator indexUpdate = new IndexInsertDeleteOperator(dataSourceIndex,
                            insertOp.getPrimaryKeyExpressions(), tokenizeKeyExprs, filterExpression,
                            insertOp.getOperation(), insertOp.isBulkload());
                    indexUpdate.setAdditionalFilteringExpressions(additionalFilteringExpressions);
                    indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(tokenUpdate));

                    context.computeAndSetTypeEnvironmentForOperator(indexUpdate);

                    currentTop = indexUpdate;
                    op0.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));

                } else {
                    // When TokenizeOperator is not needed
                    IndexInsertDeleteOperator indexUpdate = new IndexInsertDeleteOperator(dataSourceIndex,
                            insertOp.getPrimaryKeyExpressions(), secondaryExpressions, filterExpression,
                            insertOp.getOperation(), insertOp.isBulkload());
                    indexUpdate.setAdditionalFilteringExpressions(additionalFilteringExpressions);
                    indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));

                    currentTop = indexUpdate;
                    context.computeAndSetTypeEnvironmentForOperator(indexUpdate);

                    if (insertOp.isBulkload())
                        op0.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));

                }

            } else if (index.getIndexType() == IndexType.RTREE) {
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(0),
                        secondaryKeyFields.get(0), recType);
                IAType spatialType = keyPairType.first;
                int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
                int numKeys = dimension * 2;
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
                AssignOperator assignCoordinates = new AssignOperator(keyVarList, keyExprList);
                assignCoordinates.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
                context.computeAndSetTypeEnvironmentForOperator(assignCoordinates);
                // We must enforce the filter if the originating spatial type is
                // nullable.
                boolean forceFilter = keyPairType.second;
                Mutable<ILogicalExpression> filterExpression = createFilterExpression(keyVarList,
                        context.getOutputTypeEnvironment(assignCoordinates), forceFilter);
                AqlIndex dataSourceIndex = new AqlIndex(index, dataverseName, datasetName, mp);
                IndexInsertDeleteOperator indexUpdate = new IndexInsertDeleteOperator(dataSourceIndex,
                        insertOp.getPrimaryKeyExpressions(), secondaryExpressions, filterExpression,
                        insertOp.getOperation(), insertOp.isBulkload());
                indexUpdate.setAdditionalFilteringExpressions(additionalFilteringExpressions);
                indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(assignCoordinates));
                currentTop = indexUpdate;
                context.computeAndSetTypeEnvironmentForOperator(indexUpdate);

                if (insertOp.isBulkload())
                    op0.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));

            }

        }
        if (!hasSecondaryIndex) {
            return false;
        }

        if (!insertOp.isBulkload()) {
            op0.getInputs().clear();
            op0.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
        }
        return true;
    }

    public static ARecordType createEnforcedType(ARecordType initialType, Index index)
            throws AsterixException, AlgebricksException {
        ARecordType enforcedType = initialType;
        for (int i = 0; i < index.getKeyFieldNames().size(); i++) {
            try {
                Stack<Pair<ARecordType, String>> nestedTypeStack = new Stack<Pair<ARecordType, String>>();
                List<String> splits = index.getKeyFieldNames().get(i);
                ARecordType nestedFieldType = enforcedType;
                boolean openRecords = false;
                String bridgeName = nestedFieldType.getTypeName();
                int j;
                //Build the stack for the enforced type
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
                    //create the smallest record
                    enforcedType = new ARecordType(splits.get(splits.size() - 2),
                            new String[] { splits.get(splits.size() - 1) },
                            new IAType[] { AUnionType.createNullableType(index.getKeyFieldTypes().get(i)) }, true);
                    //create the open part of the nested field
                    for (int k = splits.size() - 3; k > (j - 2); k--) {
                        enforcedType = new ARecordType(splits.get(k), new String[] { splits.get(k + 1) },
                                new IAType[] { AUnionType.createNullableType(enforcedType) }, true);
                    }
                    //Bridge the gap
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
                    enforcedType = new ARecordType(nestedFieldType.getTypeName(),
                            ArrayUtils.addAll(nestedFieldType.getFieldNames(), splits.get(splits.size() - 1)),
                            ArrayUtils.addAll(nestedFieldType.getFieldTypes(),
                                    AUnionType.createNullableType(index.getKeyFieldTypes().get(i))),
                            nestedFieldType.isOpen());
                }

                //Create the enforcedtype for the nested fields in the schema, from the ground up
                if (nestedTypeStack.size() > 0) {
                    while (!nestedTypeStack.isEmpty()) {
                        Pair<ARecordType, String> nestedTypePair = nestedTypeStack.pop();
                        ARecordType nestedRecType = nestedTypePair.first;
                        IAType[] nestedRecTypeFieldTypes = nestedRecType.getFieldTypes().clone();
                        nestedRecTypeFieldTypes[nestedRecType.getFieldIndex(nestedTypePair.second)] = enforcedType;
                        enforcedType = new ARecordType(nestedRecType.getTypeName(), nestedRecType.getFieldNames(),
                                nestedRecTypeFieldTypes, nestedRecType.isOpen());
                    }
                }

            } catch (AsterixException e) {
                throw new AlgebricksException(
                        "Cannot enforce typed fields " + StringUtils.join(index.getKeyFieldNames()), e);
            } catch (IOException e) {
                throw new AsterixException(e);
            }
        }
        return enforcedType;
    }

    @SuppressWarnings("unchecked")
    private void prepareVarAndExpression(List<String> field, String[] fieldNames, LogicalVariable recordVar,
            List<Mutable<ILogicalExpression>> expressions, List<LogicalVariable> vars, IOptimizationContext context)
                    throws AlgebricksException {
        Mutable<ILogicalExpression> varRef = new MutableObject<ILogicalExpression>(
                new VariableReferenceExpression(recordVar));
        int pos = -1;
        if (field.size() == 1) {
            for (int j = 0; j < fieldNames.length; j++) {
                if (fieldNames[j].equals(field.get(0))) {
                    pos = j;
                    break;
                }
            }
        }
        if (pos == -1) {
            AbstractFunctionCallExpression func;
            if (field.size() > 1) {
                AOrderedList fieldList = new AOrderedList(new AOrderedListType(BuiltinType.ASTRING, null));
                for (int i = 0; i < field.size(); i++) {
                    fieldList.add(new AString(field.get(i)));
                }
                Mutable<ILogicalExpression> fieldRef = new MutableObject<ILogicalExpression>(
                        new ConstantExpression(new AsterixConstantValue(fieldList)));
                //Create an expression for the nested case
                func = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_NESTED), varRef, fieldRef);
            } else {
                Mutable<ILogicalExpression> fieldRef = new MutableObject<ILogicalExpression>(
                        new ConstantExpression(new AsterixConstantValue(new AString(field.get(0)))));
                //Create an expression for the open field case (By name)
                func = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME), varRef, fieldRef);
            }
            expressions.add(new MutableObject<ILogicalExpression>(func));
            LogicalVariable newVar = context.newVar();
            vars.add(newVar);
        } else {
            // Assumes the indexed field is in the closed portion of the type.
            Mutable<ILogicalExpression> indexRef = new MutableObject<ILogicalExpression>(
                    new ConstantExpression(new AsterixConstantValue(new AInt32(pos))));
            AbstractFunctionCallExpression func = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX), varRef, indexRef);
            expressions.add(new MutableObject<ILogicalExpression>(func));
            LogicalVariable newVar = context.newVar();
            vars.add(newVar);
        }
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
