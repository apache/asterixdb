/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class LeftOuterUnnestOperator extends AbstractUnnestNonMapOperator {

    public LeftOuterUnnestOperator(LogicalVariable variable, Mutable<ILogicalExpression> expression) {
        super(variable, expression);
    }

    public LeftOuterUnnestOperator(LogicalVariable variable, Mutable<ILogicalExpression> expression,
            LogicalVariable positionalVariable, Object positionalVariableType) {
        super(variable, expression, positionalVariable, positionalVariableType);
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitLeftOuterUnnestOperator(this, arg);
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        PropagatingTypeEnvironment env = createPropagatingAllInputsTypeEnvironment(ctx);

        // The produced variables of the this operator are missable because of the left outer semantics.
        Object t = env.getType(expression.getValue());
        env.setVarType(variables.get(0), ctx.getMissableTypeComputer().makeMissableType(t));
        if (positionalVariable != null) {
            env.setVarType(positionalVariable, ctx.getMissableTypeComputer().makeMissableType(positionalVariableType));
        }

        return env;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.LEFT_OUTER_UNNEST;
    }
}
