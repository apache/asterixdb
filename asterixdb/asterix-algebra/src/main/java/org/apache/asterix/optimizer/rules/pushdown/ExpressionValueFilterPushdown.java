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
package org.apache.asterix.optimizer.rules.pushdown;

import static org.apache.asterix.metadata.utils.ColumnFilterBuilder.COMPARE_FUNCTIONS;
import static org.apache.asterix.metadata.utils.ColumnFilterBuilder.PUSHABLE_FUNCTIONS;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ColumnFilterPathBuilderVisitor;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaNodeType;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.asterix.runtime.projection.ProjectionFiltrationWarningFactoryProvider;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Pushdown {@link SelectOperator} condition to the dataset to allow filtering mega leaf nodes.
 * This is currently only allowed for {@link DatasetConfig.DatasetFormat#COLUMN}
 * TODO allow partial filter with AND (e.g., lowercase(stringVal) == "some_text" AND intVal > 10 --push--> intVal > 10 )
 * TODO Filter could prevent REPLICATE (i.e., we can scan a dataset twice due to the fact one scan is filtered and
 * TODO the other is not) or (both have different filters)
 * TODO part of this class could potentially be used for external data dynamic prefixes
 */
class ExpressionValueFilterPushdown {
    private final ExpectedSchemaBuilder builder;
    private final ColumnFilterPathBuilderVisitor pathBuilder;
    private final Map<AbstractScanOperator, Map<ILogicalExpression, ARecordType>> datasetFilterPaths;
    private final Map<AbstractScanOperator, ILogicalExpression> datasetFilterExpression;
    private final Map<AbstractScanOperator, Map<String, FunctionCallInformation>> scanSourceInformationMaps;
    private final Set<AbstractScanOperator> registeredScans;
    private final boolean columnFilterEnabled;

    ExpressionValueFilterPushdown(ExpectedSchemaBuilder builder, boolean columnFilterEnabled) {
        this.builder = builder;
        pathBuilder = new ColumnFilterPathBuilderVisitor();
        datasetFilterPaths = new HashMap<>();
        datasetFilterExpression = new HashMap<>();
        scanSourceInformationMaps = new HashMap<>();
        registeredScans = new HashSet<>();
        this.columnFilterEnabled = columnFilterEnabled;
    }

    public void registerDataset(AbstractScanOperator op, DatasetDataSource source) {
        if (!columnFilterEnabled) {
            return;
        }

        Dataset dataset = source.getDataset();
        if (dataset.getDatasetType() == DatasetConfig.DatasetType.INTERNAL
                && dataset.getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.COLUMN) {
            registeredScans.add(op);
        }
    }

    /**
     * Try to push the condition down to dataset scan/access
     *
     * @param selectOp the select operator
     */
    public void addFilterExpression(SelectOperator selectOp, AbstractScanOperator scanOp) {
        if (datasetFilterPaths.containsKey(scanOp)) {
            // Most bottom SELECT was applied, other selects should be ignored
            return;
        }
        Map<ILogicalExpression, ARecordType> filterPaths = new HashMap<>();
        Map<String, FunctionCallInformation> sourceInformationMap = new HashMap<>();
        Mutable<ILogicalExpression> conditionRef = selectOp.getCondition();
        if (addPaths(conditionRef, filterPaths, sourceInformationMap)) {
            datasetFilterPaths.put(scanOp, filterPaths);
            datasetFilterExpression.put(scanOp, conditionRef.getValue());
            scanSourceInformationMaps.put(scanOp, sourceInformationMap);
        }
    }

    public Map<ILogicalExpression, ARecordType> getFilterPaths(AbstractScanOperator scanOp) {
        return datasetFilterPaths.getOrDefault(scanOp, Collections.emptyMap());
    }

    public ILogicalExpression getFilterExpression(AbstractScanOperator scanOp) {
        return datasetFilterExpression.get(scanOp);
    }

    public Map<String, FunctionCallInformation> getSourceInformationMap(AbstractScanOperator scanOp) {
        return scanSourceInformationMaps.getOrDefault(scanOp, new HashMap<>());
    }

    private boolean addPaths(Mutable<ILogicalExpression> exprRef, Map<ILogicalExpression, ARecordType> filterPaths,
            Map<String, FunctionCallInformation> sourceInformationMap) {
        ILogicalExpression expr = exprRef.getValue();
        if (!isFunctionExpression(expr)) {
            return false;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fid = ((AbstractFunctionCallExpression) expr).getFunctionIdentifier();

        if (!PUSHABLE_FUNCTIONS.contains(fid)) {
            return false;
        }

        if (COMPARE_FUNCTIONS.contains(fid)) {
            return addPaths(funcExpr, filterPaths, sourceInformationMap);
        }
        //AND/OR descend to the expression tree
        return addPathsForArgs(funcExpr, filterPaths, sourceInformationMap);
    }

    private boolean addPaths(AbstractFunctionCallExpression funcExpr, Map<ILogicalExpression, ARecordType> filterPaths,
            Map<String, FunctionCallInformation> sourceInformationMap) {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();

        ILogicalExpression left = args.get(0).getValue();
        ILogicalExpression right = args.get(1).getValue();

        if (isConstantExpression(left)) {
            return addPaths(funcExpr, right, left, filterPaths, sourceInformationMap, true);
        } else if (isConstantExpression(right)) {
            return addPaths(funcExpr, left, right, filterPaths, sourceInformationMap, false);
        }
        // No constants, return false
        return false;
    }

    private boolean addPaths(AbstractFunctionCallExpression funcExpr, ILogicalExpression columnExpr,
            ILogicalExpression constExpr, Map<ILogicalExpression, ARecordType> filterPaths,
            Map<String, FunctionCallInformation> sourceInformationMap, boolean leftConstant) {
        IExpectedSchemaNode node;
        if (isFunctionExpression(columnExpr)) {
            node = builder.getNode((AbstractFunctionCallExpression) columnExpr);
        } else {
            //Variable
            node = builder.getNode(((VariableReferenceExpression) columnExpr).getVariableReference());
        }

        if (node == null || node.getType() != ExpectedSchemaNodeType.ANY) {
            // Expression cannot be pushed (e.g., $$r.getField("x") + 1) or had been accessed as a nested value
            // Bail out
            return false;
        }

        AnyExpectedSchemaNode leafNode = (AnyExpectedSchemaNode) node;
        IAObject constantValue = ((AsterixConstantValue) ((ConstantExpression) constExpr).getValue()).getObject();

        String functionName = funcExpr.getFunctionIdentifier().getName();
        SourceLocation sourceLocation = funcExpr.getSourceLocation();
        FunctionCallInformation functionCallInfo = new FunctionCallInformation(functionName, sourceLocation,
                ProjectionFiltrationWarningFactoryProvider.getIncomparableTypesFactory(leftConstant));

        ARecordType path = pathBuilder.buildPath(leafNode, constantValue, sourceInformationMap, functionCallInfo);
        filterPaths.put(columnExpr, path);
        return true;
    }

    private boolean addPathsForArgs(AbstractFunctionCallExpression funcExpr,
            Map<ILogicalExpression, ARecordType> filterPaths,
            Map<String, FunctionCallInformation> sourceInformationMap) {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        boolean add = true;
        for (int i = 0; add && i < args.size(); i++) {
            add = addPaths(args.get(i), filterPaths, sourceInformationMap);
        }
        return add;
    }

    private static boolean isFunctionExpression(ILogicalExpression expr) {
        return expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL;
    }

    private static boolean isConstantExpression(ILogicalExpression expr) {
        return expr.getExpressionTag() == LogicalExpressionTag.CONSTANT;
    }

    public boolean allowsPushdown(AbstractScanOperator lastSeenScan) {
        return columnFilterEnabled && lastSeenScan != null && registeredScans.contains(lastSeenScan);
    }
}
