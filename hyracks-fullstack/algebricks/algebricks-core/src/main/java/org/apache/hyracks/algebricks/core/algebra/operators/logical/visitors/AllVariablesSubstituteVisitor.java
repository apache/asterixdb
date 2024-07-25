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
package org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

/**
 * Substitute all variables with the provided one
 * Example substitute all variables with $$myVar:
 * add($$1, $$2) --> add($$myVar, $$myVar)
 */
public class AllVariablesSubstituteVisitor implements ILogicalExpressionVisitor<Void, LogicalVariable> {
    @Override
    public Void visitConstantExpression(ConstantExpression expr, LogicalVariable arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitVariableReferenceExpression(VariableReferenceExpression expr, LogicalVariable arg)
            throws AlgebricksException {
        expr.setVariable(arg);
        return null;
    }

    @Override
    public Void visitAggregateFunctionCallExpression(AggregateFunctionCallExpression expr, LogicalVariable arg)
            throws AlgebricksException {
        visitArgs(expr.getArguments(), arg);
        return null;
    }

    @Override
    public Void visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr, LogicalVariable arg)
            throws AlgebricksException {
        visitArgs(expr.getArguments(), arg);
        return null;
    }

    @Override
    public Void visitStatefulFunctionCallExpression(StatefulFunctionCallExpression expr, LogicalVariable arg)
            throws AlgebricksException {
        visitArgs(expr.getArguments(), arg);
        return null;
    }

    @Override
    public Void visitUnnestingFunctionCallExpression(UnnestingFunctionCallExpression expr, LogicalVariable arg)
            throws AlgebricksException {
        visitArgs(expr.getArguments(), arg);
        return null;
    }

    private void visitArgs(List<Mutable<ILogicalExpression>> args, LogicalVariable variable)
            throws AlgebricksException {
        for (Mutable<ILogicalExpression> funcArg : args) {
            funcArg.getValue().accept(this, variable);
        }
    }
}
