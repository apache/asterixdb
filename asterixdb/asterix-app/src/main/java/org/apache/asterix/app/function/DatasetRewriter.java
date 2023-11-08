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

import static org.apache.asterix.common.api.IIdentifierMapper.Modifier.PLURAL;
import static org.apache.asterix.common.api.IIdentifierMapper.Modifier.SINGULAR;
import static org.apache.asterix.common.utils.IdentifierUtil.dataset;
import static org.apache.asterix.external.util.ExternalDataConstants.SUBPATH;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.functions.IFunctionToDataSourceRewriter;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.UnnestToDataScanRule;
import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
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
                    "No positional variables are allowed over " + dataset(PLURAL));
        }

        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        Dataset dataset = fetchDataset(metadataProvider, f);
        List<LogicalVariable> variables = new ArrayList<>();
        switch (dataset.getDatasetType()) {
            case INTERNAL:
                int numPrimaryKeys = dataset.getPrimaryKeys().size();
                for (int i = 0; i < numPrimaryKeys; i++) {
                    variables.add(context.newVar());
                }
                break;
            case EXTERNAL:
                break;
            default:
                // VIEWS are not expected at this point
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, unnest.getSourceLocation(),
                        String.format("Unexpected %s type %s for %s %s", dataset(SINGULAR), dataset.getDatasetType(),
                                dataset(SINGULAR), DatasetUtil.getFullyQualifiedDisplayName(dataset)));
        }
        variables.add(unnest.getVariable());

        DataSourceId dsid =
                new DataSourceId(dataset.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName());
        DataSource dataSource = metadataProvider.findDataSource(dsid);
        boolean hasMeta = dataSource.hasMeta();
        if (hasMeta) {
            variables.add(context.newVar());
        }
        DataSourceScanOperator scan = new DataSourceScanOperator(variables, dataSource);
        scan.setSourceLocation(unnest.getSourceLocation());
        if (dataset.getDatasetType() == DatasetConfig.DatasetType.EXTERNAL) {
            Map<String, Object> unnestAnnotations = unnest.getAnnotations();
            scan.getAnnotations().putAll(unnestAnnotations);
            Map<String, Serializable> dataSourceProperties = dataSource.getProperties();
            Object externalSubpath = unnestAnnotations.get(SUBPATH);
            if (externalSubpath instanceof String) {
                dataSourceProperties.put(SUBPATH, (String) externalSubpath);
            }
        }
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
        AbstractFunctionCallExpression datasetFnCall = (AbstractFunctionCallExpression) expression;
        MetadataProvider metadata = (MetadataProvider) mp;
        Dataset dataset = fetchDataset(metadata, datasetFnCall);
        IAType type = metadata.findType(dataset.getItemTypeDatabaseName(), dataset.getItemTypeDataverseName(),
                dataset.getItemTypeName());
        if (type == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, datasetFnCall.getSourceLocation(),
                    "No type for " + dataset() + " " + dataset.getDatasetName());
        }
        return type;
    }

    public static Dataset fetchDataset(MetadataProvider metadataProvider, AbstractFunctionCallExpression datasetFnCall)
            throws CompilationException {
        DatasetFullyQualifiedName datasetReference = FunctionUtil.parseDatasetFunctionArguments(datasetFnCall);
        DataverseName dataverseName = datasetReference.getDataverseName();
        String database = datasetReference.getDatabaseName();
        String datasetName = datasetReference.getDatasetName();
        Dataset dataset;
        try {
            dataset = metadataProvider.findDataset(database, dataverseName, datasetName);
        } catch (CompilationException e) {
            throw e;
        } catch (AlgebricksException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, e, datasetFnCall.getSourceLocation(),
                    e.getMessage());
        }
        if (dataset == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, datasetFnCall.getSourceLocation(),
                    datasetName,
                    MetadataUtil.dataverseName(database, dataverseName, metadataProvider.isUsingDatabase()));
        }
        return dataset;
    }
}
