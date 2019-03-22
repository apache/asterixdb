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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * Left-outer-unnest-map is similar to the unnest-map operator. The only
 * difference is that this operator represents left-outer semantics, meaning
 * that it generates null values for non-matching tuples. It also propagates all
 * input variables. This may be used only in a left-outer-join case.
 */
public class LeftOuterUnnestMapOperator extends AbstractUnnestMapOperator {

    public LeftOuterUnnestMapOperator(List<LogicalVariable> variables, Mutable<ILogicalExpression> expression,
            List<Object> variableTypes, boolean propagateInput) {
        super(variables, expression, variableTypes, propagateInput);
        // propagateInput is always set to true for this operator.
        this.propagateInput = true;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitLeftOuterUnnestMapOperator(this, arg);
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        // Propagates all input variables that come from the outer branch.
        PropagatingTypeEnvironment env = createPropagatingAllInputsTypeEnvironment(ctx);

        env.getCorrelatedMissableVariableLists().add(new ArrayList<>(variables));

        // For the variables from the inner branch, the output type is the union
        // of (original type + null).
        for (int i = 0; i < variables.size(); i++) {
            env.setVarType(variables.get(i), ctx.getMissableTypeComputer().makeMissableType(variableTypes.get(i)));
        }

        return env;
    }

}
