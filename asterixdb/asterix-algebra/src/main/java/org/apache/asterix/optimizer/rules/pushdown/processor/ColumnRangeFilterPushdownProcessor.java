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
package org.apache.asterix.optimizer.rules.pushdown.processor;

import static org.apache.asterix.column.filter.range.accessor.ConstantColumnRangeFilterValueAccessorFactory.SUPPORTED_CONSTANT_TYPES;
import static org.apache.asterix.metadata.utils.PushdownUtil.RANGE_FILTER_PUSHABLE_FUNCTIONS;
import static org.apache.asterix.metadata.utils.PushdownUtil.isConstant;
import static org.apache.asterix.metadata.utils.PushdownUtil.isFilterPath;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.UseDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaNodeType;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.asterix.runtime.projection.ProjectionFiltrationWarningFactoryProvider;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Computes a range-filter expression for {@link DatasetConfig.DatasetFormat#COLUMN} datasets. Each column is such
 * dataset contains a pair of normalized min-max values. The range filter expression can be utilized to filter out
 * mega leaf nodes that do not satisfy the range filter expression.
 */
public class ColumnRangeFilterPushdownProcessor extends ColumnFilterPushdownProcessor {
    private final Map<String, FunctionCallInformation> sourceInformationMap;

    public ColumnRangeFilterPushdownProcessor(PushdownContext pushdownContext, IOptimizationContext context) {
        super(pushdownContext, context);
        sourceInformationMap = new HashMap<>();
    }

    @Override
    protected boolean skip(ScanDefineDescriptor scanDefineDescriptor) {
        Dataset dataset = scanDefineDescriptor.getDataset();
        return dataset.getDatasetFormatInfo().getFormat() != DatasetConfig.DatasetFormat.COLUMN
                || !DatasetUtil.isRangeFilterPushdownSupported(dataset);
    }

    @Override
    protected void preparePushdown(UseDescriptor useDescriptor, ScanDefineDescriptor scanDescriptor)
            throws AlgebricksException {
        super.preparePushdown(useDescriptor, scanDescriptor);
        sourceInformationMap.clear();
    }

    @Override
    protected boolean isNotPushable(AbstractFunctionCallExpression expression) {
        return !RANGE_FILTER_PUSHABLE_FUNCTIONS.contains(expression.getFunctionIdentifier());
    }

    @Override
    protected boolean handleCompare(AbstractFunctionCallExpression expression) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = expression.getArguments();

        Mutable<ILogicalExpression> leftRef = args.get(0);
        Mutable<ILogicalExpression> rightRef = args.get(1);

        ILogicalExpression left = leftRef.getValue();
        ILogicalExpression right = rightRef.getValue();

        if (isConstant(left) && isFilterPath(right)) {
            return pushdownRangeFilter(right, left, expression, true);
        } else if (isConstant(right) && isFilterPath(left)) {
            return pushdownRangeFilter(left, right, expression, false);
        }
        // Either it is a compare that doesn't involve a constant there's a function that wraps the value access path
        return false;
    }

    @Override
    protected boolean handlePath(AbstractFunctionCallExpression expression) throws AlgebricksException {
        // This means we got something like WHERE $r.getField("isVerified") -- where isVerified is a boolean field.
        AnyExpectedSchemaNode node = getNode(expression);
        IAObject constantValue = ABoolean.TRUE;
        String functionName = expression.getFunctionIdentifier().getName();
        SourceLocation sourceLocation = expression.getSourceLocation();
        FunctionCallInformation functionCallInfo = new FunctionCallInformation(functionName, sourceLocation,
                ProjectionFiltrationWarningFactoryProvider.getIncomparableTypesFactory(false));
        ARecordType path =
                pathBuilderVisitor.buildPath(node, constantValue.getType(), sourceInformationMap, functionCallInfo);
        paths.put(expression, path);
        return true;
    }

    @Override
    protected void putFilterInformation(ScanDefineDescriptor scanDefineDescriptor, ILogicalExpression inlinedExpr) {
        ILogicalExpression filterExpr = scanDefineDescriptor.getRangeFilterExpression();
        if (filterExpr != null) {
            filterExpr = andExpression(filterExpr, inlinedExpr);
            scanDefineDescriptor.setRangeFilterExpression(filterExpr);
        } else {
            scanDefineDescriptor.setRangeFilterExpression(inlinedExpr);
        }
        scanDefineDescriptor.getFilterPaths().putAll(paths);
        scanDefineDescriptor.getPathLocations().putAll(sourceInformationMap);
    }

    private boolean pushdownRangeFilter(ILogicalExpression pathExpr, ILogicalExpression constExpr,
            AbstractFunctionCallExpression funcExpr, boolean leftConstant) throws AlgebricksException {
        AnyExpectedSchemaNode node = getNode(pathExpr);
        IAObject constantValue = ((AsterixConstantValue) ((ConstantExpression) constExpr).getValue()).getObject();
        if (node == null || !SUPPORTED_CONSTANT_TYPES.contains(constantValue.getType().getTypeTag())) {
            return false;
        }
        String functionName = funcExpr.getFunctionIdentifier().getName();
        SourceLocation sourceLocation = funcExpr.getSourceLocation();
        FunctionCallInformation functionCallInfo = new FunctionCallInformation(functionName, sourceLocation,
                ProjectionFiltrationWarningFactoryProvider.getIncomparableTypesFactory(leftConstant));
        ARecordType path =
                pathBuilderVisitor.buildPath(node, constantValue.getType(), sourceInformationMap, functionCallInfo);
        paths.put(pathExpr, path);
        return true;
    }

    private AnyExpectedSchemaNode getNode(ILogicalExpression expression) throws AlgebricksException {
        IExpectedSchemaNode node = expression.accept(exprToNodeVisitor, null);
        if (node == null || node.getType() != ExpectedSchemaNodeType.ANY) {
            return null;
        }
        return (AnyExpectedSchemaNode) node;
    }

}
