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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.NonPropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class UnnestMapOperator extends AbstractUnnestMapOperator {

    // the select condition in the SELECT operator. Only results satisfying this selectCondition
    // would be returned by this operator
    private Mutable<ILogicalExpression> selectCondition;
    // the maximum of number of results output by this operator
    private long outputLimit = -1;

    public UnnestMapOperator(List<LogicalVariable> variables, Mutable<ILogicalExpression> expression,
            List<Object> variableTypes, boolean propagateInput) {
        this(variables, expression, variableTypes, propagateInput, null, -1);
    }

    public UnnestMapOperator(List<LogicalVariable> variables, Mutable<ILogicalExpression> expression,
            List<Object> variableTypes, boolean propagateInput, Mutable<ILogicalExpression> selectCondition,
            long outputLimit) {
        super(variables, expression, variableTypes, propagateInput);
        this.selectCondition = selectCondition;
        this.outputLimit = outputLimit;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.UNNEST_MAP;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitUnnestMapOperator(this, arg);
    }

    // When propagateInput is true,
    // this operator propagates all input variables.
    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env;
        if (propagateInput) {
            env = createPropagatingAllInputsTypeEnvironment(ctx);
        } else {
            env = new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMetadataProvider());
        }
        int n = variables.size();
        for (int i = 0; i < n; i++) {
            env.setVarType(variables.get(i), variableTypes.get(i));
        }
        return env;
    }

    public Mutable<ILogicalExpression> getSelectCondition() {
        return selectCondition;
    }

    public void setSelectCondition(Mutable<ILogicalExpression> selectCondition) {
        this.selectCondition = selectCondition;
    }

    public long getOutputLimit() {
        return outputLimit;
    }

    public void setOutputLimit(long outputLimit) {
        this.outputLimit = outputLimit;
    }

}
