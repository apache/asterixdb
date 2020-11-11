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
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.functions.IFunctionToDataSourceRewriter;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.UnnestToDataScanRule;
import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
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
        if (f == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, opRef.getValue().getSourceLocation(),
                    "");
        }
        UnnestOperator unnest = (UnnestOperator) opRef.getValue();
        if (unnest.getPositionalVariable() != null) {
            // TODO remove this after enabling the support of positional variables in data scan
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, unnest.getSourceLocation(),
                    "No positional variables are allowed over datasets.");
        }

        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        Pair<DataverseName, String> datasetReference = FunctionUtil.parseDatasetFunctionArguments(f);
        DataverseName dataverseName = datasetReference.first;
        String datasetName = datasetReference.second;
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, unnest.getSourceLocation(),
                    datasetName, dataverseName);
        }
        DataSourceId dsid = new DataSourceId(dataset.getDataverseName(), dataset.getDatasetName());
        List<LogicalVariable> variables = new ArrayList<>();
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            int numPrimaryKeys = dataset.getPrimaryKeys().size();
            for (int i = 0; i < numPrimaryKeys; i++) {
                variables.add(context.newVar());
            }
        }
        variables.add(unnest.getVariable());
        DataSource dataSource = metadataProvider.findDataSource(dsid);
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
        MetadataProvider metadata = (MetadataProvider) mp;
        Pair<DataverseName, String> datasetInfo = FunctionUtil.parseDatasetFunctionArguments(f);
        DataverseName dataverseName = datasetInfo.first;
        String datasetName = datasetInfo.second;
        Dataset dataset = metadata.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, f.getSourceLocation(), datasetName,
                    dataverseName);
        }
        String tn = dataset.getItemTypeName();
        IAType t2 = metadata.findType(dataset.getItemTypeDataverseName(), tn);
        if (t2 == null) {
            throw new AlgebricksException("No type for dataset " + datasetName);
        }
        return t2;
    }
}
