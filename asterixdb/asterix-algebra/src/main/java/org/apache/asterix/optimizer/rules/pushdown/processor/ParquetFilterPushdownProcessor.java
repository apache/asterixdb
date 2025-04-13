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

import static org.apache.asterix.metadata.utils.PushdownUtil.RANGE_FILTER_PUSHABLE_FUNCTIONS;

import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ParquetDatasetScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaNodeType;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public class ParquetFilterPushdownProcessor extends ColumnFilterPushdownProcessor {

    public ParquetFilterPushdownProcessor(PushdownContext pushdownContext, IOptimizationContext context) {
        super(pushdownContext, context);
    }

    @Override
    protected boolean skip(ScanDefineDescriptor scanDefineDescriptor) throws AlgebricksException {
        return !DatasetUtil.isParquetFormat(scanDefineDescriptor.getDataset());
    }

    @Override
    protected boolean isNotPushable(AbstractFunctionCallExpression expression) {
        return !RANGE_FILTER_PUSHABLE_FUNCTIONS.contains(expression.getFunctionIdentifier());
    }

    @Override
    protected boolean handlePath(AbstractFunctionCallExpression expression, IExpectedSchemaNode node)
            throws AlgebricksException {
        if (node.getType() != ExpectedSchemaNodeType.ANY) {
            return false;
        }

        // The inferred path from the provided expression
        ARecordType expressionPath = pathBuilderVisitor.buildPath((AnyExpectedSchemaNode) node);
        paths.put(expression, expressionPath);
        return true;
    }

    @Override
    protected void putFilterInformation(ScanDefineDescriptor scanDefineDescriptor, ILogicalExpression inlinedExpr)
            throws AlgebricksException {
        if (checkerVisitor.containsMultipleArrayPaths(paths.values())) {
            // Cannot pushdown a filter with multiple unnest
            // TODO allow rewindable column readers for filters
            // TODO this is a bit conservative (maybe too conservative) as we can push part of expression down
            return;
        }
        ParquetDatasetScanDefineDescriptor scanDefDesc = (ParquetDatasetScanDefineDescriptor) scanDefineDescriptor;
        ILogicalExpression filterExpr = scanDefDesc.getRowGroupFilterExpression();
        if (filterExpr != null) {
            filterExpr = andExpression(filterExpr, inlinedExpr);
            scanDefDesc.setRowGroupFilterExpression(filterExpr);
        } else {
            scanDefDesc.setRowGroupFilterExpression(inlinedExpr);
        }
    }
}
