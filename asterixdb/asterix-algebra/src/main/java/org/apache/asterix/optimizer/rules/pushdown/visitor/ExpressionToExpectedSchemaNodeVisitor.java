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
package org.apache.asterix.optimizer.rules.pushdown.visitor;

import static org.apache.asterix.metadata.utils.PushdownUtil.FILTER_PUSHABLE_PATH_FUNCTIONS;

import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.schema.AbstractComplexExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaBuilder;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaNodeType;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.RootExpectedSchemaNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

public class ExpressionToExpectedSchemaNodeVisitor implements ILogicalExpressionVisitor<IExpectedSchemaNode, Void> {
    private IVariableTypeEnvironment typeEnv;

    private ScanDefineDescriptor scanDefineDescriptor;

    public void reset(ScanDefineDescriptor scanDefineDescriptor) {
        this.scanDefineDescriptor = scanDefineDescriptor;
    }

    public void setTypeEnv(IVariableTypeEnvironment typeEnv) {
        this.typeEnv = typeEnv;
    }

    @Override
    public IExpectedSchemaNode visitConstantExpression(ConstantExpression expr, Void arg) throws AlgebricksException {
        return null;
    }

    @Override
    public IExpectedSchemaNode visitVariableReferenceExpression(VariableReferenceExpression expr, Void arg)
            throws AlgebricksException {
        LogicalVariable variable = VariableUtilities.getVariable(expr);
        if (scanDefineDescriptor.getVariable() == variable
                || scanDefineDescriptor.getMetaRecordVariable() == variable) {
            return RootExpectedSchemaNode.ALL_FIELDS_ROOT_NODE;
        }
        return null;
    }

    @Override
    public IExpectedSchemaNode visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        return handleFunction(expr);
    }

    // Disabled expressions
    @Override
    public IExpectedSchemaNode visitAggregateFunctionCallExpression(AggregateFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        return null;
    }

    @Override
    public IExpectedSchemaNode visitStatefulFunctionCallExpression(StatefulFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        return null;
    }

    @Override
    public IExpectedSchemaNode visitUnnestingFunctionCallExpression(UnnestingFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        return handleFunction(expr);
    }

    private IExpectedSchemaNode handleFunction(AbstractFunctionCallExpression expr) throws AlgebricksException {
        FunctionIdentifier fid = expr.getFunctionIdentifier();
        if (!FILTER_PUSHABLE_PATH_FUNCTIONS.contains(fid)) {
            // If not a supported function, return null
            return null;
        }

        // All supported functions have the node at their first argument
        ILogicalExpression parentExpr = expr.getArguments().get(0).getValue();
        IExpectedSchemaNode parent = parentExpr.accept(this, null);
        if (parent == null) {
            return null;
        }

        AbstractComplexExpectedSchemaNode newParent = replaceIfNeeded(parent, expr);
        IExpectedSchemaNode myNode =
                new AnyExpectedSchemaNode(newParent, expr.getSourceLocation(), expr.getFunctionIdentifier().getName());
        ExpectedSchemaBuilder.addChild(expr, typeEnv, newParent, myNode);
        return myNode;
    }

    private AbstractComplexExpectedSchemaNode replaceIfNeeded(IExpectedSchemaNode parent,
            AbstractFunctionCallExpression funcExpr) {
        ExpectedSchemaNodeType expectedType = ExpectedSchemaBuilder.getExpectedNestedNodeType(funcExpr);
        return (AbstractComplexExpectedSchemaNode) parent.replaceIfNeeded(expectedType, parent.getSourceLocation(),
                parent.getFunctionName());
    }
}
