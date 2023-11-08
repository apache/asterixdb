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

import static org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier.VARARGS;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.ISecondaryIndexOperationsHelper;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtil;
import org.apache.asterix.metadata.utils.SecondaryIndexOperationsHelper;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.LogRedactionUtil;

public class QueryIndexRewriter extends FunctionRewriter implements IResultTypeComputer {

    public static final FunctionIdentifier QUERY_INDEX = FunctionConstants.newAsterix("query-index", VARARGS);
    public static final QueryIndexRewriter INSTANCE = new QueryIndexRewriter(QUERY_INDEX);

    private QueryIndexRewriter(FunctionIdentifier functionId) {
        super(functionId);
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env, IMetadataProvider<?, ?> mp)
            throws AlgebricksException {
        return computeRecType((AbstractFunctionCallExpression) expression, (MetadataProvider) mp, null, null, null);
    }

    @Override
    public FunctionDataSource toDatasource(IOptimizationContext ctx, AbstractFunctionCallExpression f)
            throws AlgebricksException {
        final SourceLocation loc = f.getSourceLocation();
        DataverseName dvName = getDataverseName(loc, f.getArguments(), 0);
        String dsName = getString(loc, f.getArguments(), 1);
        String idName = getString(loc, f.getArguments(), 2);
        String dbName;
        if (f.getArguments().size() > 3) {
            dbName = getString(loc, f.getArguments(), 3);
        } else {
            dbName = MetadataUtil.databaseFor(dvName);
        }
        MetadataProvider mp = (MetadataProvider) ctx.getMetadataProvider();
        final Dataset dataset = validateDataset(mp, dbName, dvName, dsName, loc);
        Index index = validateIndex(f, mp, loc, dbName, dvName, dsName, idName);
        return createQueryIndexDatasource(mp, dataset, index, loc, f);
    }

    @Override
    protected void createDataScanOp(Mutable<ILogicalOperator> opRef, UnnestOperator unnest, IOptimizationContext ctx,
            AbstractFunctionCallExpression f) throws AlgebricksException {
        FunctionDataSource datasource = toDatasource(ctx, f);
        List<LogicalVariable> variables = new ArrayList<>();
        List<Mutable<ILogicalExpression>> closedRecArgs = new ArrayList<>();
        MetadataProvider mp = (MetadataProvider) ctx.getMetadataProvider();
        computeRecType(f, mp, variables, closedRecArgs, ctx);
        DataSourceScanOperator scan = new DataSourceScanOperator(variables, datasource);
        scan.setSourceLocation(unnest.getSourceLocation());
        List<Mutable<ILogicalOperator>> scanInpList = scan.getInputs();
        scanInpList.addAll(unnest.getInputs());
        ScalarFunctionCallExpression recordCreationFunc = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR), closedRecArgs);
        recordCreationFunc.setSourceLocation(unnest.getSourceLocation());
        AssignOperator assignOp = new AssignOperator(unnest.getVariable(), new MutableObject<>(recordCreationFunc));
        assignOp.getInputs().add(new MutableObject<>(scan));
        assignOp.setSourceLocation(unnest.getSourceLocation());
        ctx.computeAndSetTypeEnvironmentForOperator(scan);
        ctx.computeAndSetTypeEnvironmentForOperator(assignOp);
        opRef.setValue(assignOp);
    }

    @Override
    protected boolean invalidArgs(List<Mutable<ILogicalExpression>> args) {
        return args.size() < 3;
    }

    private FunctionDataSource createQueryIndexDatasource(MetadataProvider mp, Dataset ds, Index idx,
            SourceLocation loc, AbstractFunctionCallExpression f) throws AlgebricksException {
        ISecondaryIndexOperationsHelper secIdxHelper =
                SecondaryIndexOperationsHelper.createIndexOperationsHelper(ds, idx, mp, loc);
        AlgebricksAbsolutePartitionConstraint secPartitionConstraint =
                (AlgebricksAbsolutePartitionConstraint) secIdxHelper.getSecondaryPartitionConstraint();
        INodeDomain domain = mp.findNodeDomain(ds.getNodeGroupName());
        ARecordType recType = computeRecType(f, mp, null, null, null);
        int numSecKeys = ((Index.ValueIndexDetails) idx.getIndexDetails()).getKeyFieldNames().size();
        return new QueryIndexDatasource(ds, idx.getIndexName(), domain, secPartitionConstraint, recType, numSecKeys);
    }

    private ARecordType computeRecType(AbstractFunctionCallExpression f, MetadataProvider metadataProvider,
            List<LogicalVariable> outVars, List<Mutable<ILogicalExpression>> closedRecArgs,
            IOptimizationContext context) throws AlgebricksException {
        final SourceLocation loc = f.getSourceLocation();
        DataverseName dataverseName = getDataverseName(loc, f.getArguments(), 0);
        String datasetName = getString(loc, f.getArguments(), 1);
        String indexName = getString(loc, f.getArguments(), 2);
        String databaseName;
        if (f.getArguments().size() > 3) {
            databaseName = getString(loc, f.getArguments(), 3);
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        Dataset dataset = validateDataset(metadataProvider, databaseName, dataverseName, datasetName, loc);
        Index index = validateIndex(f, metadataProvider, loc, databaseName, dataverseName, datasetName, indexName);
        ARecordType dsType = (ARecordType) metadataProvider.findType(dataset);
        ARecordType metaType = DatasetUtil.getMetaType(metadataProvider, dataset);
        dsType = (ARecordType) metadataProvider.findTypeForDatasetWithoutType(dsType, metaType, dataset);

        List<IAType> dsKeyTypes = KeyFieldTypeUtil.getPartitoningKeyTypes(dataset, dsType, metaType);
        List<Pair<IAType, Boolean>> secKeyTypes = KeyFieldTypeUtil.getBTreeIndexKeyTypes(index, dsType, metaType);
        int numPrimaryKeys = dsKeyTypes.size();
        int numSecKeys = secKeyTypes.size();
        String[] fieldNames = new String[numSecKeys + numPrimaryKeys];
        IAType[] fieldTypes = new IAType[numSecKeys + numPrimaryKeys];
        int keyIdx = 0;
        boolean overridingKeyFieldTypes = index.getIndexDetails().isOverridingKeyFieldTypes();
        for (int i = 0; i < numSecKeys; i++, keyIdx++) {
            IAType secKeyType = secKeyTypes.get(i).first;
            Boolean makeOptional = secKeyTypes.get(i).second;
            fieldTypes[keyIdx] =
                    overridingKeyFieldTypes || makeOptional ? AUnionType.createUnknownableType(secKeyType) : secKeyType;
            fieldNames[keyIdx] = "SK" + i;
            setAssignVarsExprs(outVars, closedRecArgs, context, loc, fieldNames, keyIdx);
        }
        for (int k = 0; k < numPrimaryKeys; k++, keyIdx++) {
            fieldTypes[keyIdx] = dsKeyTypes.get(k);
            fieldNames[keyIdx] = "PK" + k;
            setAssignVarsExprs(outVars, closedRecArgs, context, loc, fieldNames, keyIdx);
        }
        return new ARecordType("", fieldNames, fieldTypes, false);
    }

    private void setAssignVarsExprs(List<LogicalVariable> outVars, List<Mutable<ILogicalExpression>> closedRecArgs,
            IOptimizationContext context, SourceLocation loc, String[] fieldNames, int n) {
        if (context != null) {
            LogicalVariable logicalVariable = context.newVar();
            outVars.add(logicalVariable);
            ConstantExpression nameExpr = new ConstantExpression(new AsterixConstantValue(new AString(fieldNames[n])));
            VariableReferenceExpression varRefExpr = new VariableReferenceExpression(logicalVariable);
            nameExpr.setSourceLocation(loc);
            varRefExpr.setSourceLocation(loc);
            closedRecArgs.add(new MutableObject<>(nameExpr));
            closedRecArgs.add(new MutableObject<>(varRefExpr));
        }
    }

    private static Dataset validateDataset(MetadataProvider mp, String dbName, DataverseName dvName, String dsName,
            SourceLocation loc) throws AlgebricksException {
        Dataset dataset = mp.findDataset(dbName, dvName, dsName);
        if (dataset == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, loc, dsName,
                    MetadataUtil.dataverseName(dbName, dvName, mp.isUsingDatabase()));
        }
        return dataset;
    }

    private static Index validateIndex(AbstractFunctionCallExpression f, MetadataProvider mp, SourceLocation loc,
            String database, DataverseName dvName, String dsName, String idxName) throws AlgebricksException {
        Index index = mp.getIndex(database, dvName, dsName, idxName);
        if (index == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_INDEX, loc, idxName);
        }
        if (index.isPrimaryIndex()) {
            throw new CompilationException(ErrorCode.OPERATION_NOT_SUPPORTED_ON_PRIMARY_INDEX, loc, idxName);
        }
        DatasetConfig.IndexType idxType = index.getIndexType();
        // currently, only normal secondary indexes are supported
        if (idxType != DatasetConfig.IndexType.BTREE || Index.IndexCategory.of(idxType) != Index.IndexCategory.VALUE
                || index.isPrimaryKeyIndex()) {
            throw new CompilationException(ErrorCode.COMPILATION_FUNC_EXPRESSION_CANNOT_UTILIZE_INDEX,
                    f.getSourceLocation(), LogRedactionUtil.userData(f.toString()));
        }
        return index;
    }
}
