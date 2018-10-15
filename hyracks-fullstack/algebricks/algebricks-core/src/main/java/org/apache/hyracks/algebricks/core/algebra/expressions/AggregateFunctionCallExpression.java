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
package org.apache.hyracks.algebricks.core.algebra.expressions;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

/**
 * An aggregate function may be executed in a "two step" mode. First the
 * "step-one" aggregates are run and then the results are passed to the
 * "step-two" aggregators. The convention is the following:
 * 1. The step-one aggregate must be able to accept the same arguments as the
 * original aggregate function call.
 * 2. The step-two aggregate must be a unary function that accepts as input the
 * output of the step-one aggregate.
 */

public class AggregateFunctionCallExpression extends AbstractFunctionCallExpression {

    private boolean twoStep;
    private IFunctionInfo stepOneAggregate;
    private IFunctionInfo stepTwoAggregate;

    public AggregateFunctionCallExpression(IFunctionInfo finfo, boolean isTwoStep,
            List<Mutable<ILogicalExpression>> arguments) {
        super(FunctionKind.AGGREGATE, finfo, arguments);
        this.twoStep = isTwoStep;
    }

    public boolean isTwoStep() {
        return twoStep;
    }

    @Override
    public AggregateFunctionCallExpression cloneExpression() {
        cloneAnnotations();
        List<Mutable<ILogicalExpression>> clonedArgs = cloneArguments();
        AggregateFunctionCallExpression fun = new AggregateFunctionCallExpression(finfo, twoStep, clonedArgs);
        fun.setStepTwoAggregate(stepTwoAggregate);
        fun.setStepOneAggregate(stepOneAggregate);
        fun.setSourceLocation(sourceLoc);
        // opaqueParameters are not really cloned
        fun.setOpaqueParameters(getOpaqueParameters());
        return fun;
    }

    public void setStepOneAggregate(IFunctionInfo stepOneAggregate) {
        this.stepOneAggregate = stepOneAggregate;
    }

    public IFunctionInfo getStepOneAggregate() {
        return stepOneAggregate;
    }

    public void setStepTwoAggregate(IFunctionInfo stepTwoAggregate) {
        this.stepTwoAggregate = stepTwoAggregate;
    }

    public IFunctionInfo getStepTwoAggregate() {
        return stepTwoAggregate;
    }

    @Override
    public <R, T> R accept(ILogicalExpressionVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitAggregateFunctionCallExpression(this, arg);
    }

}
