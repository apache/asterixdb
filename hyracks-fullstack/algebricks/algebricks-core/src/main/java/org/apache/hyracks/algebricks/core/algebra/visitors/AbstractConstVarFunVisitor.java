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
package org.apache.hyracks.algebricks.core.algebra.visitors;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;

public abstract class AbstractConstVarFunVisitor<R, T> implements ILogicalExpressionVisitor<R, T> {

    public abstract R visitFunctionCallExpression(AbstractFunctionCallExpression expr, T arg)
            throws AlgebricksException;

    public R visitAggregateFunctionCallExpression(AggregateFunctionCallExpression expr, T arg)
            throws AlgebricksException {
        return visitFunctionCallExpression(expr, arg);
    }

    public R visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr, T arg) throws AlgebricksException {
        return visitFunctionCallExpression(expr, arg);
    }

    public R visitStatefulFunctionCallExpression(StatefulFunctionCallExpression expr, T arg)
            throws AlgebricksException {
        return visitFunctionCallExpression(expr, arg);
    }

    public R visitUnnestingFunctionCallExpression(UnnestingFunctionCallExpression expr, T arg)
            throws AlgebricksException {
        return visitFunctionCallExpression(expr, arg);
    }
}
