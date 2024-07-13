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

import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtil;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
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

/**
 * query-partition("db", "dv", "ds", 0);
 * query-partition("dv", "ds", 0);
 */
public class QueryPartitionRewriter extends FunctionRewriter implements IResultTypeComputer {

    public static final FunctionIdentifier QUERY_PARTITION = FunctionConstants.newAsterix("query-partition", VARARGS);
    public static final QueryPartitionRewriter INSTANCE = new QueryPartitionRewriter(QUERY_PARTITION);

    private QueryPartitionRewriter(FunctionIdentifier functionId) {
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
        int numArgs = f.getArguments().size();
        int nextArg = 0;
        if (numArgs > 3) {
            nextArg++;
        }
        DataverseName dvName = getDataverseName(loc, f.getArguments(), nextArg++);
        String dsName = getString(loc, f.getArguments(), nextArg++);
        Long partitionNum = ConstantExpressionUtil.getLongArgument(f, nextArg);
        if (partitionNum == null) {
            throw new IllegalArgumentException("partition number should be a number");
        }
        String dbName;
        if (numArgs > 3) {
            dbName = getString(loc, f.getArguments(), 0);
        } else {
            dbName = MetadataUtil.databaseFor(dvName);
        }
        MetadataProvider mp = (MetadataProvider) ctx.getMetadataProvider();
        final Dataset dataset = validateDataset(mp, dbName, dvName, dsName, loc);
        return createQueryPartitionDatasource(mp, dataset, partitionNum.intValue(), loc, f);
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

    private FunctionDataSource createQueryPartitionDatasource(MetadataProvider mp, Dataset ds, int partitionNum,
            SourceLocation loc, AbstractFunctionCallExpression f) throws AlgebricksException {
        INodeDomain domain = mp.findNodeDomain(ds.getNodeGroupName());
        PartitioningProperties partitioningProperties = mp.getPartitioningProperties(ds);
        AlgebricksPartitionConstraint constraints = partitioningProperties.getConstraints();
        ARecordType recType = computeRecType(f, mp, null, null, null);
        return new QueryPartitionDatasource(ds, domain, (AlgebricksAbsolutePartitionConstraint) constraints, recType,
                partitionNum);
    }

    private ARecordType computeRecType(AbstractFunctionCallExpression f, MetadataProvider metadataProvider,
            List<LogicalVariable> outVars, List<Mutable<ILogicalExpression>> closedRecArgs,
            IOptimizationContext context) throws AlgebricksException {
        final SourceLocation loc = f.getSourceLocation();
        int numArgs = f.getArguments().size();
        int nextArg = 0;
        if (numArgs > 3) {
            nextArg++;
        }
        DataverseName dvName = getDataverseName(loc, f.getArguments(), nextArg++);
        String dsName = getString(loc, f.getArguments(), nextArg++);
        String dbName;
        if (numArgs > 3) {
            dbName = getString(loc, f.getArguments(), 0);
        } else {
            dbName = MetadataUtil.databaseFor(dvName);
        }
        Dataset dataset = validateDataset(metadataProvider, dbName, dvName, dsName, loc);
        ARecordType dsType = (ARecordType) metadataProvider.findType(dataset);
        ARecordType metaType = DatasetUtil.getMetaType(metadataProvider, dataset);
        dsType = (ARecordType) metadataProvider.findTypeForDatasetWithoutType(dsType, metaType, dataset);

        List<IAType> dsKeyTypes = KeyFieldTypeUtil.getPartitoningKeyTypes(dataset, dsType, metaType);
        List<List<String>> primaryKeys = dataset.getPrimaryKeys();
        int numPrimaryKeys = dsKeyTypes.size();
        int numPayload = metaType == null ? 1 : 2;
        String[] fieldNames = new String[numPrimaryKeys + numPayload];
        IAType[] fieldTypes = new IAType[numPrimaryKeys + numPayload];
        int keyIdx = 0;
        for (int k = 0; k < numPrimaryKeys; k++, keyIdx++) {
            fieldTypes[keyIdx] = dsKeyTypes.get(k);
            fieldNames[keyIdx] = StringUtils.join(primaryKeys.get(k), ".");
            setAssignVarsExprs(outVars, closedRecArgs, context, loc, fieldNames, keyIdx);
        }
        fieldTypes[keyIdx] = dsType;
        fieldNames[keyIdx] = "rec";
        setAssignVarsExprs(outVars, closedRecArgs, context, loc, fieldNames, keyIdx);
        if (metaType != null) {
            keyIdx++;
            fieldTypes[keyIdx] = metaType;
            fieldNames[keyIdx] = "meta";
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
}
