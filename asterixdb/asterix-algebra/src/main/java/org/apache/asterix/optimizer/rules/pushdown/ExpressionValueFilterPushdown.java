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

import static org.apache.asterix.metadata.utils.filter.NormalizedColumnFilterBuilder.COMPARE_FUNCTIONS;
import static org.apache.asterix.metadata.utils.filter.NormalizedColumnFilterBuilder.NORMALIZED_PUSHABLE_FUNCTIONS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.filter.ColumnFilterBuilder;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ColumnFilterPathBuilderVisitor;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaNodeType;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.asterix.runtime.projection.ProjectionFiltrationWarningFactoryProvider;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Pushdown {@link SelectOperator} condition to the dataset to allow filtering mega leaf nodes.
 * This is currently only allowed for {@link DatasetConfig.DatasetFormat#COLUMN}
 * TODO Filter could prevent REPLICATE (i.e., we can scan a dataset twice due to the fact one scan is filtered and
 * TODO the other is not or both have different filters)
 * TODO part of this class could potentially be used for external data dynamic prefixes
 */
class ExpressionValueFilterPushdown {
    private final ExpectedSchemaBuilder builder;
    private final ColumnFilterPathBuilderVisitor pathBuilder;
    private final Map<AbstractScanOperator, Map<ILogicalExpression, ARecordType>> normalizedFilterPaths;
    private final Map<AbstractScanOperator, Map<ILogicalExpression, ARecordType>> actualFilterPaths;
    private final Map<AbstractScanOperator, ILogicalExpression> datasetFilterExpression;
    private final Map<AbstractScanOperator, Map<String, FunctionCallInformation>> scanSourceInformationMaps;
    private final Set<AbstractScanOperator> registeredScans;
    private final HashMap<LogicalVariable, ILogicalExpression> registeredExpressions;
    private final boolean columnFilterEnabled;

    ExpressionValueFilterPushdown(ExpectedSchemaBuilder builder, boolean columnFilterEnabled) {
        this.builder = builder;
        pathBuilder = new ColumnFilterPathBuilderVisitor();
        normalizedFilterPaths = new HashMap<>();
        actualFilterPaths = new HashMap<>();
        datasetFilterExpression = new HashMap<>();
        scanSourceInformationMaps = new HashMap<>();
        registeredScans = new HashSet<>();
        registeredExpressions = new HashMap<>();
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

    public void registerExpression(LogicalVariable producedVar, ILogicalExpression expr) {
        if (builder.getNode(expr) == null) {
            // we only register expressions that do not correspond to a schema node
            registeredExpressions.put(producedVar, expr);
        }
    }

    /**
     * Try to push the condition down to dataset scan/access
     *
     * @param selectOp the select operator
     */
    public void addFilterExpression(IOptimizationContext context, SelectOperator selectOp,
            AbstractScanOperator scanOp) {
        Map<ILogicalExpression, ARecordType> normalizedPaths = new HashMap<>();
        Map<ILogicalExpression, ARecordType> actualPaths = new HashMap<>();
        Map<String, FunctionCallInformation> sourceInformationMap = new HashMap<>();
        ILogicalExpression conditionClone = selectOp.getCondition().getValue().cloneExpression();
        Mutable<ILogicalExpression> conditionRef = new MutableObject<>(conditionClone);
        if (addPaths(conditionRef, normalizedPaths, actualPaths, sourceInformationMap, true)) {
            // Normalized paths
            Map<ILogicalExpression, ARecordType> allNormalizedPaths =
                    normalizedFilterPaths.computeIfAbsent(scanOp, k -> new HashMap<>());
            allNormalizedPaths.putAll(normalizedPaths);

            // Actual paths
            Map<ILogicalExpression, ARecordType> allActualPaths =
                    actualFilterPaths.computeIfAbsent(scanOp, k -> new HashMap<>());
            allActualPaths.putAll(actualPaths);

            // OR the previous condition with the current condition (if any)
            // This might bring more than what's actually satisfies the predicate
            putExpression(context, scanOp, conditionClone);
            scanSourceInformationMaps.put(scanOp, sourceInformationMap);
        }
    }

    private void putExpression(IOptimizationContext context, AbstractScanOperator scanOp,
            ILogicalExpression conditionExpr) {
        ILogicalExpression filterExpr = datasetFilterExpression.get(scanOp);
        if (filterExpr != null) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) filterExpr;
            if (!BuiltinFunctions.OR.equals(funcExpr.getFunctionIdentifier())) {
                IFunctionInfo fInfo = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.OR);
                List<Mutable<ILogicalExpression>> args = new ArrayList<>();
                args.add(new MutableObject<>(filterExpr));
                funcExpr = new ScalarFunctionCallExpression(fInfo, args);
                filterExpr = funcExpr;
            }
            funcExpr.getArguments().add(new MutableObject<>(conditionExpr));
        } else {
            filterExpr = conditionExpr;
        }
        datasetFilterExpression.put(scanOp, filterExpr);
    }

    public Map<ILogicalExpression, ARecordType> getNormalizedFilterPaths(AbstractScanOperator scanOp) {
        return normalizedFilterPaths.getOrDefault(scanOp, Collections.emptyMap());
    }

    public Map<ILogicalExpression, ARecordType> getActualFilterPaths(AbstractScanOperator scanOp) {
        return actualFilterPaths.getOrDefault(scanOp, Collections.emptyMap());
    }

    public ILogicalExpression getFilterExpression(AbstractScanOperator scanOp) {
        return datasetFilterExpression.get(scanOp);
    }

    public Map<String, FunctionCallInformation> getSourceInformationMap(AbstractScanOperator scanOp) {
        return scanSourceInformationMaps.getOrDefault(scanOp, new HashMap<>());
    }

    private boolean addPaths(Mutable<ILogicalExpression> exprRef, Map<ILogicalExpression, ARecordType> normalizedPaths,
            Map<ILogicalExpression, ARecordType> actualPaths, Map<String, FunctionCallInformation> sourceInformationMap,
            boolean includeNormalized) {
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            IAObject constantValue = ((AsterixConstantValue) ((ConstantExpression) expr).getValue()).getObject();
            // Continue if a primitive constant is encountered
            return !constantValue.getType().getTypeTag().isDerivedType();
        }

        LogicalVariable variable = VariableUtilities.getVariable(expr);
        if (variable != null && registeredExpressions.containsKey(variable)) {
            // Inline the expression
            ILogicalExpression currentExpr = registeredExpressions.get(variable);
            exprRef.setValue(currentExpr);
            return addPaths(exprRef, normalizedPaths, actualPaths, sourceInformationMap, false);
        }

        IExpectedSchemaNode node = builder.getNode(expr);
        if (node != null) {
            return addPath(node, expr, actualPaths);
        }

        if (!isFunctionExpression(expr)) {
            return false;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fid = ((AbstractFunctionCallExpression) expr).getFunctionIdentifier();
        if (!ColumnFilterBuilder.isPushable(fid)) {
            return false;
        }

        boolean normalizedIncluded = includeNormalized && NORMALIZED_PUSHABLE_FUNCTIONS.contains(fid);

        if (COMPARE_FUNCTIONS.contains(fid)) {
            return handleCompare(funcExpr, normalizedPaths, actualPaths, sourceInformationMap, normalizedIncluded);
        }
        //AND/OR descend to the expression tree
        return handleArgs(funcExpr, normalizedPaths, actualPaths, sourceInformationMap, includeNormalized);
    }

    private boolean handleCompare(AbstractFunctionCallExpression funcExpr,
            Map<ILogicalExpression, ARecordType> normalizedPaths, Map<ILogicalExpression, ARecordType> actualPaths,
            Map<String, FunctionCallInformation> sourceInformationMap, boolean includeNormalized) {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();

        Mutable<ILogicalExpression> leftRef = args.get(0);
        Mutable<ILogicalExpression> rightRef = args.get(1);

        ILogicalExpression left = leftRef.getValue();
        ILogicalExpression right = rightRef.getValue();

        if (isConstantExpression(left)) {
            return handleCompare(funcExpr, rightRef, left, normalizedPaths, actualPaths, sourceInformationMap,
                    includeNormalized, true);
        } else if (isConstantExpression(right)) {
            return handleCompare(funcExpr, leftRef, right, normalizedPaths, actualPaths, sourceInformationMap,
                    includeNormalized, false);
        }
        // No constants, return false
        return false;
    }

    private boolean handleCompare(AbstractFunctionCallExpression funcExpr, Mutable<ILogicalExpression> columnExprRef,
            ILogicalExpression constExpr, Map<ILogicalExpression, ARecordType> normalizedPaths,
            Map<ILogicalExpression, ARecordType> actualPaths, Map<String, FunctionCallInformation> sourceInformationMap,
            boolean includeNormalized, boolean leftConstant) {

        IAObject constantValue = ((AsterixConstantValue) ((ConstantExpression) constExpr).getValue()).getObject();
        if (constantValue.getType().getTypeTag().isDerivedType()) {
            // Cannot compare against nested values
            return false;
        }

        ILogicalExpression columnExpr = columnExprRef.getValue();
        IExpectedSchemaNode node = builder.getNode(columnExpr);
        if (node == null || node.getType() != ExpectedSchemaNodeType.ANY) {
            // Handle as a nested function call (e.g., numeric-add($$x, 1) > 10) where $$x is a value path
            return addPaths(columnExprRef, normalizedPaths, actualPaths, null, false);
        }

        AnyExpectedSchemaNode leafNode = (AnyExpectedSchemaNode) node;

        String functionName = funcExpr.getFunctionIdentifier().getName();
        SourceLocation sourceLocation = funcExpr.getSourceLocation();
        FunctionCallInformation functionCallInfo = new FunctionCallInformation(functionName, sourceLocation,
                ProjectionFiltrationWarningFactoryProvider.getIncomparableTypesFactory(leftConstant));

        ARecordType path = pathBuilder.buildPath(leafNode, constantValue, sourceInformationMap, functionCallInfo);
        if (includeNormalized
                && (!normalizedPaths.containsKey(columnExpr) || path.equals(normalizedPaths.get(columnExpr)))) {
            normalizedPaths.put(columnExpr, path);
        } else {
            normalizedPaths.clear();
        }
        actualPaths.put(columnExpr, path);
        return true;
    }

    private boolean addPath(IExpectedSchemaNode node, ILogicalExpression columnExpr,
            Map<ILogicalExpression, ARecordType> actualPaths) {
        if (node.getType() != ExpectedSchemaNodeType.ANY) {
            return false;
        }
        AnyExpectedSchemaNode leafNode = (AnyExpectedSchemaNode) node;
        ARecordType path = pathBuilder.buildPath(leafNode, null, null, null);
        actualPaths.put(columnExpr, path);
        return true;
    }

    private boolean handleArgs(AbstractFunctionCallExpression funcExpr,
            Map<ILogicalExpression, ARecordType> normalizedPaths, Map<ILogicalExpression, ARecordType> actualPaths,
            Map<String, FunctionCallInformation> sourceInformationMap, boolean includeNormalized) {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        boolean add = true;
        for (int i = 0; add && i < args.size(); i++) {
            add = addPaths(args.get(i), normalizedPaths, actualPaths, sourceInformationMap, includeNormalized);
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
