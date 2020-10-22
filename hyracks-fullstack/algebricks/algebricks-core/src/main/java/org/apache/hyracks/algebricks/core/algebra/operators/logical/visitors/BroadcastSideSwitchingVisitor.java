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
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

public class BroadcastSideSwitchingVisitor implements ILogicalExpressionVisitor<ILogicalExpression, Void> {
    private static BroadcastSideSwitchingVisitor instance = null;

    private BroadcastSideSwitchingVisitor() {
    }

    public static BroadcastSideSwitchingVisitor getInstance() {
        if (instance == null) {
            instance = new BroadcastSideSwitchingVisitor();
        }
        return instance;
    }

    @Override
    public ILogicalExpression visitConstantExpression(ConstantExpression expr, Void arg) {
        return null;
    }

    @Override
    public ILogicalExpression visitVariableReferenceExpression(VariableReferenceExpression expr, Void arg) {
        return null;
    }

    @Override
    public ILogicalExpression visitAggregateFunctionCallExpression(AggregateFunctionCallExpression expr, Void arg) {
        return null;
    }

    @Override
    public ILogicalExpression visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr, Void arg)
            throws AlgebricksException {
        FunctionIdentifier fi = expr.getFunctionIdentifier();
        if (fi.equals(AlgebricksBuiltinFunctions.AND)) {
            for (Mutable<ILogicalExpression> a : expr.getArguments()) {
                a.getValue().accept(this, null);
            }
        }
        BroadcastExpressionAnnotation bcastAnn = expr.removeAnnotation(BroadcastExpressionAnnotation.class);
        if (bcastAnn != null) {
            BroadcastExpressionAnnotation.BroadcastSide oppositeSide =
                    BroadcastExpressionAnnotation.BroadcastSide.getOppositeSide(bcastAnn.getBroadcastSide());
            expr.putAnnotation(new BroadcastExpressionAnnotation(oppositeSide));
        }
        return null;
    }

    @Override
    public ILogicalExpression visitStatefulFunctionCallExpression(StatefulFunctionCallExpression expr, Void arg) {
        return null;
    }

    @Override
    public ILogicalExpression visitUnnestingFunctionCallExpression(UnnestingFunctionCallExpression expr, Void arg) {
        return null;
    }
}
