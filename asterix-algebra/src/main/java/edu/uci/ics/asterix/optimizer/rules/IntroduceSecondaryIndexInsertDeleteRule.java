/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.metadata.declared.AqlIndex;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroduceSecondaryIndexInsertDeleteRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
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

        /** op2 is the assign operator which extract primary keys from the record variable */
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();

        if (recordVar.size() == 0) {
            /**
             * For the case primary key-assignment expressions are constant expressions,
             * find assign op that creates record to be inserted/deleted.
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
        String datasetName = datasetSource.getId().getDatasetName();
        Dataset dataset = mp.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return false;
        }

        List<LogicalVariable> projectVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getUsedVariables(op1, projectVars);
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
        for (Index index : indexes) {
            if (!index.isSecondaryIndex()) {
                continue;
            }
            hasSecondaryIndex = true;
            List<String> secondaryKeyFields = index.getKeyFieldNames();
            List<LogicalVariable> secondaryKeyVars = new ArrayList<LogicalVariable>();
            List<Mutable<ILogicalExpression>> expressions = new ArrayList<Mutable<ILogicalExpression>>();
            List<Mutable<ILogicalExpression>> secondaryExpressions = new ArrayList<Mutable<ILogicalExpression>>();
            for (String secondaryKey : secondaryKeyFields) {
                Mutable<ILogicalExpression> varRef = new MutableObject<ILogicalExpression>(
                        new VariableReferenceExpression(recordVar.get(0)));
                String[] fieldNames = recType.getFieldNames();
                int pos = -1;
                for (int j = 0; j < fieldNames.length; j++) {
                    if (fieldNames[j].equals(secondaryKey)) {
                        pos = j;
                        break;
                    }
                }
                // Assumes the indexed field is in the closed portion of the type.
                Mutable<ILogicalExpression> indexRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                        new AsterixConstantValue(new AInt32(pos))));
                AbstractFunctionCallExpression func = new ScalarFunctionCallExpression(
                        FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX), varRef, indexRef);
                expressions.add(new MutableObject<ILogicalExpression>(func));
                LogicalVariable newVar = context.newVar();
                secondaryKeyVars.add(newVar);
            }

            AssignOperator assign = new AssignOperator(secondaryKeyVars, expressions);
            ProjectOperator project = new ProjectOperator(projectVars);
            assign.getInputs().add(new MutableObject<ILogicalOperator>(project));
            project.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
            context.computeAndSetTypeEnvironmentForOperator(project);
            context.computeAndSetTypeEnvironmentForOperator(assign);
            if (index.getIndexType() == IndexType.BTREE || index.getIndexType() == IndexType.SINGLE_PARTITION_WORD_INVIX
                    || index.getIndexType() == IndexType.SINGLE_PARTITION_NGRAM_INVIX
                    || index.getIndexType() == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                    || index.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
                for (LogicalVariable secondaryKeyVar : secondaryKeyVars) {
                    secondaryExpressions.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                            secondaryKeyVar)));
                }
                Mutable<ILogicalExpression> filterExpression = createFilterExpression(secondaryKeyVars,
                        context.getOutputTypeEnvironment(assign), false);
                AqlIndex dataSourceIndex = new AqlIndex(index, dataverseName, datasetName, mp);
                IndexInsertDeleteOperator indexUpdate = new IndexInsertDeleteOperator(dataSourceIndex,
                        insertOp.getPrimaryKeyExpressions(), secondaryExpressions, filterExpression,
                        insertOp.getOperation());
                indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                currentTop = indexUpdate;
                context.computeAndSetTypeEnvironmentForOperator(indexUpdate);
            } else if (index.getIndexType() == IndexType.RTREE) {
                Pair<IAType, Boolean> keyPairType = Index
                        .getNonNullableKeyFieldType(secondaryKeyFields.get(0), recType);
                IAType spatialType = keyPairType.first;
                int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
                int numKeys = dimension * 2;
                List<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
                List<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
                for (int i = 0; i < numKeys; i++) {
                    LogicalVariable keyVar = context.newVar();
                    keyVarList.add(keyVar);
                    AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                            FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CREATE_MBR));
                    createMBR.getArguments().add(
                            new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVars
                                    .get(0))));
                    createMBR.getArguments().add(
                            new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                                    new AInt32(dimension)))));
                    createMBR.getArguments().add(
                            new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                                    new AInt32(i)))));
                    keyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
                }
                for (LogicalVariable secondaryKeyVar : keyVarList) {
                    secondaryExpressions.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                            secondaryKeyVar)));
                }
                AssignOperator assignCoordinates = new AssignOperator(keyVarList, keyExprList);
                assignCoordinates.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                context.computeAndSetTypeEnvironmentForOperator(assignCoordinates);
                // We must enforce the filter if the originating spatial type is nullable.
                boolean forceFilter = keyPairType.second;
                Mutable<ILogicalExpression> filterExpression = createFilterExpression(keyVarList,
                        context.getOutputTypeEnvironment(assignCoordinates), forceFilter);
                AqlIndex dataSourceIndex = new AqlIndex(index, dataverseName, datasetName, mp);
                IndexInsertDeleteOperator indexUpdate = new IndexInsertDeleteOperator(dataSourceIndex,
                        insertOp.getPrimaryKeyExpressions(), secondaryExpressions, filterExpression,
                        insertOp.getOperation());
                indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(assignCoordinates));
                currentTop = indexUpdate;
                context.computeAndSetTypeEnvironmentForOperator(indexUpdate);
            }
        }
        if (!hasSecondaryIndex) {
            return false;
        }
        op0.getInputs().clear();
        op0.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
        return true;
    }

    @SuppressWarnings("unchecked")
    private Mutable<ILogicalExpression> createFilterExpression(List<LogicalVariable> secondaryKeyVars,
            IVariableTypeEnvironment typeEnv, boolean forceFilter) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> filterExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        // Add 'is not null' to all nullable secondary index keys as a filtering condition.
        for (LogicalVariable secondaryKeyVar : secondaryKeyVars) {
            IAType secondaryKeyType = (IAType) typeEnv.getVarType(secondaryKeyVar);
            if (!isNullableType(secondaryKeyType) && !forceFilter) {
                continue;
            }
            ScalarFunctionCallExpression isNullFuncExpr = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.IS_NULL),
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVar)));
            ScalarFunctionCallExpression notFuncExpr = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.NOT), new MutableObject<ILogicalExpression>(
                            isNullFuncExpr));
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
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.AND), filterExpressions));
        } else {
            filterExpression = filterExpressions.get(0);
        }
        return filterExpression;
    }

    private boolean isNullableType(IAType type) {
        if (type.getTypeTag() == ATypeTag.UNION) {
            return ((AUnionType) type).isNullableType();
        }
        return false;
    }
}
