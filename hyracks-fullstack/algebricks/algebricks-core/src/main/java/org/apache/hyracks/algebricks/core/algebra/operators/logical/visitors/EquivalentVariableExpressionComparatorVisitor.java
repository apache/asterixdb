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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

/**
 * Allows to compare two expression with the assumption that all variables are equivalent (e.g., two different
 * variables originated from two different data-scan operators but on the same dataset)
 */
public class EquivalentVariableExpressionComparatorVisitor
        implements ILogicalExpressionVisitor<Boolean, ILogicalExpression> {
    public static final EquivalentVariableExpressionComparatorVisitor INSTANCE =
            new EquivalentVariableExpressionComparatorVisitor();

    private EquivalentVariableExpressionComparatorVisitor() {
    }

    @Override
    public Boolean visitConstantExpression(ConstantExpression expr, ILogicalExpression arg) throws AlgebricksException {
        return expr.equals(arg);
    }

    @Override
    public Boolean visitVariableReferenceExpression(VariableReferenceExpression expr, ILogicalExpression arg)
            throws AlgebricksException {
        if (arg.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }

    @Override
    public Boolean visitAggregateFunctionCallExpression(AggregateFunctionCallExpression expr, ILogicalExpression arg)
            throws AlgebricksException {
        return equals(expr, arg);
    }

    @Override
    public Boolean visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr, ILogicalExpression arg)
            throws AlgebricksException {
        return equals(expr, arg);
    }

    @Override
    public Boolean visitStatefulFunctionCallExpression(StatefulFunctionCallExpression expr, ILogicalExpression arg)
            throws AlgebricksException {
        return equals(expr, arg);
    }

    @Override
    public Boolean visitUnnestingFunctionCallExpression(UnnestingFunctionCallExpression expr, ILogicalExpression arg)
            throws AlgebricksException {
        return equals(expr, arg);
    }

    public Boolean equals(AbstractFunctionCallExpression expr1, ILogicalExpression expr2) throws AlgebricksException {
        if (!(expr2 instanceof AbstractFunctionCallExpression)) {
            return false;
        } else {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr2;
            boolean equal = expr1.getFunctionIdentifier().equals(fce.getFunctionIdentifier());
            if (!equal) {
                return false;
            }
            List<Mutable<ILogicalExpression>> arguments = expr1.getArguments();
            int argumentCount = arguments.size();
            List<Mutable<ILogicalExpression>> fceArguments = fce.getArguments();
            if (argumentCount != fceArguments.size()) {
                return false;
            }
            for (int i = 0; i < argumentCount; i++) {
                ILogicalExpression argument = arguments.get(i).getValue();
                ILogicalExpression fceArgument = fceArguments.get(i).getValue();
                if (argument.accept(this, fceArgument) == Boolean.FALSE) {
                    return false;
                }
            }
            return Arrays.deepEquals(expr1.getOpaqueParameters(), fce.getOpaqueParameters());
        }
    }
}
