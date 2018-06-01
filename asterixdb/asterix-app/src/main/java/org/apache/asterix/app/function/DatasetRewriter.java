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
package org.apache.asterix.app.function;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.IFunctionToDataSourceRewriter;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.UnnestToDataScanRule;
import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;

public class DatasetRewriter implements IFunctionToDataSourceRewriter, IResultTypeComputer {
    public static final DatasetRewriter INSTANCE = new DatasetRewriter();

    private DatasetRewriter() {
    }

    @Override
    public boolean rewrite(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractFunctionCallExpression f = UnnestToDataScanRule.getFunctionCall(opRef);
        UnnestOperator unnest = (UnnestOperator) opRef.getValue();
        if (unnest.getPositionalVariable() != null) {
            // TODO remove this after enabling the support of positional variables in data scan
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, unnest.getSourceLocation(),
                    "No positional variables are allowed over datasets.");
        }
        ILogicalExpression expr = f.getArguments().get(0).getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        ConstantExpression ce = (ConstantExpression) expr;
        IAlgebricksConstantValue acv = ce.getValue();
        if (!(acv instanceof AsterixConstantValue)) {
            return false;
        }
        AsterixConstantValue acv2 = (AsterixConstantValue) acv;
        if (acv2.getObject().getType().getTypeTag() != ATypeTag.STRING) {
            return false;
        }
        String datasetArg = ((AString) acv2.getObject()).getStringValue();
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        Pair<String, String> datasetReference = parseDatasetReference(metadataProvider, datasetArg);
        String dataverseName = datasetReference.first;
        String datasetName = datasetReference.second;
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, unnest.getSourceLocation(),
                    datasetName, dataverseName);
        }
        DataSourceId asid = new DataSourceId(dataverseName, datasetName);
        List<LogicalVariable> variables = new ArrayList<>();
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            int numPrimaryKeys = dataset.getPrimaryKeys().size();
            for (int i = 0; i < numPrimaryKeys; i++) {
                variables.add(context.newVar());
            }
        }
        variables.add(unnest.getVariable());
        DataSource dataSource = metadataProvider.findDataSource(asid);
        boolean hasMeta = dataSource.hasMeta();
        if (hasMeta) {
            variables.add(context.newVar());
        }
        DataSourceScanOperator scan = new DataSourceScanOperator(variables, dataSource);
        scan.setSourceLocation(unnest.getSourceLocation());
        List<Mutable<ILogicalOperator>> scanInpList = scan.getInputs();
        scanInpList.addAll(unnest.getInputs());
        opRef.setValue(scan);
        addPrimaryKey(variables, dataSource, context);
        context.computeAndSetTypeEnvironmentForOperator(scan);
        // Adds equivalence classes --- one equivalent class between a primary key
        // variable and a record field-access expression.
        IAType[] schemaTypes = dataSource.getSchemaTypes();
        ARecordType recordType =
                (ARecordType) (hasMeta ? schemaTypes[schemaTypes.length - 2] : schemaTypes[schemaTypes.length - 1]);
        ARecordType metaRecordType = (ARecordType) (hasMeta ? schemaTypes[schemaTypes.length - 1] : null);
        EquivalenceClassUtils.addEquivalenceClassesForPrimaryIndexAccess(scan, variables, recordType, metaRecordType,
                dataset, context);
        return true;
    }

    private Pair<String, String> parseDatasetReference(MetadataProvider metadataProvider, String datasetArg)
            throws AlgebricksException {
        String[] datasetNameComponents = datasetArg.split("\\.");
        String dataverseName;
        String datasetName;
        if (datasetNameComponents.length == 1) {
            Dataverse defaultDataverse = metadataProvider.getDefaultDataverse();
            if (defaultDataverse == null) {
                throw new AlgebricksException("Unresolved dataset " + datasetArg + " Dataverse not specified.");
            }
            dataverseName = defaultDataverse.getDataverseName();
            datasetName = datasetNameComponents[0];
        } else {
            dataverseName = datasetNameComponents[0];
            datasetName = datasetNameComponents[1];
        }
        return new Pair<>(dataverseName, datasetName);
    }

    private void addPrimaryKey(List<LogicalVariable> scanVariables, DataSource dataSource,
            IOptimizationContext context) {
        List<LogicalVariable> primaryKey = dataSource.getPrimaryKeyVariables(scanVariables);
        List<LogicalVariable> tail = new ArrayList<>();
        tail.addAll(scanVariables);
        FunctionalDependency pk = new FunctionalDependency(primaryKey, tail);
        context.addPrimaryKey(pk);
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env, IMetadataProvider<?, ?> mp)
            throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        if (f.getArguments().size() != 1) {
            throw new AlgebricksException("dataset arity is 1, not " + f.getArguments().size());
        }
        ILogicalExpression a1 = f.getArguments().get(0).getValue();
        IAType t1 = (IAType) env.getType(a1);
        if (t1.getTypeTag() == ATypeTag.ANY) {
            return BuiltinType.ANY;
        }
        if (t1.getTypeTag() != ATypeTag.STRING) {
            throw new AlgebricksException("Illegal type " + t1 + " for dataset() argument.");
        }
        String datasetArg = ConstantExpressionUtil.getStringConstant(a1);
        if (datasetArg == null) {
            return BuiltinType.ANY;
        }
        MetadataProvider metadata = (MetadataProvider) mp;
        Pair<String, String> datasetInfo = DatasetUtil.getDatasetInfo(metadata, datasetArg);
        String dataverseName = datasetInfo.first;
        String datasetName = datasetInfo.second;
        if (dataverseName == null) {
            throw new AlgebricksException("Unspecified dataverse!");
        }
        Dataset dataset = metadata.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Could not find dataset " + datasetName + " in dataverse " + dataverseName);
        }
        String tn = dataset.getItemTypeName();
        IAType t2 = metadata.findType(dataset.getItemTypeDataverseName(), tn);
        if (t2 == null) {
            throw new AlgebricksException("No type for dataset " + datasetName);
        }
        return t2;

    }
}
