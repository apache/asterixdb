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

import static org.apache.asterix.metadata.utils.PushdownUtil.ARRAY_FUNCTIONS;

import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaNodeType;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class DeltaTableFilterPushdownProcessor extends ColumnFilterPushdownProcessor {

    public DeltaTableFilterPushdownProcessor(PushdownContext pushdownContext, IOptimizationContext context) {
        super(pushdownContext, context);
    }

    @Override
    protected boolean skip(ScanDefineDescriptor scanDefineDescriptor) throws AlgebricksException {
        return !DatasetUtil.isDeltaTable(scanDefineDescriptor.getDataset());
    }

    @Override
    protected boolean isNotPushable(AbstractFunctionCallExpression expression) {
        FunctionIdentifier fid = expression.getFunctionIdentifier();
        return ARRAY_FUNCTIONS.contains(fid) || super.isNotPushable(expression);
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
}
