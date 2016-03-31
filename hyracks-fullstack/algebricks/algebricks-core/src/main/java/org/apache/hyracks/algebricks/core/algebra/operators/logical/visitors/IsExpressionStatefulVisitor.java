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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

/**
 * This visitor determines whether a logical expression contains any stateful
 * function call expression.
 */
public class IsExpressionStatefulVisitor implements ILogicalExpressionVisitor<Boolean, Void> {

    @Override
    public Boolean visitConstantExpression(ConstantExpression expr, Void arg) throws AlgebricksException {
        return false;
    }

    @Override
    public Boolean visitVariableReferenceExpression(VariableReferenceExpression expr, Void arg)
            throws AlgebricksException {
        return false;
    }

    @Override
    public Boolean visitAggregateFunctionCallExpression(AggregateFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        return visitFunctionExpression(expr, arg);
    }

    @Override
    public Boolean visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        return visitFunctionExpression(expr, arg);
    }

    @Override
    public Boolean visitStatefulFunctionCallExpression(StatefulFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        return true;
    }

    @Override
    public Boolean visitUnnestingFunctionCallExpression(UnnestingFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        return visitFunctionExpression(expr, arg);
    }

    private boolean visitFunctionExpression(AbstractFunctionCallExpression expr, Void arg) throws AlgebricksException {
        for (Mutable<ILogicalExpression> argRef : expr.getArguments()) {
            if (argRef.getValue().accept(this, arg)) {
                return true;
            }
        }
        return false;
    }
}
