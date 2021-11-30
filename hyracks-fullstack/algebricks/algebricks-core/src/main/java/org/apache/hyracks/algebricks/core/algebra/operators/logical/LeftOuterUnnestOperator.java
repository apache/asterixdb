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
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.api.exceptions.ErrorCode;

public class LeftOuterUnnestOperator extends AbstractUnnestNonMapOperator {

    private IAlgebricksConstantValue missingValue;

    public LeftOuterUnnestOperator(LogicalVariable variable, Mutable<ILogicalExpression> expression,
            IAlgebricksConstantValue missingValue) {
        super(variable, expression);
        setMissingValue(missingValue);
    }

    public LeftOuterUnnestOperator(LogicalVariable variable, Mutable<ILogicalExpression> expression,
            LogicalVariable positionalVariable, Object positionalVariableType, IAlgebricksConstantValue missingValue) {
        super(variable, expression, positionalVariable, positionalVariableType);
        setMissingValue(missingValue);
    }

    public IAlgebricksConstantValue getMissingValue() {
        return missingValue;
    }

    public void setMissingValue(IAlgebricksConstantValue missingValue) {
        this.missingValue = validateMissingValue(missingValue);
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitLeftOuterUnnestOperator(this, arg);
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        PropagatingTypeEnvironment env = createPropagatingAllInputsTypeEnvironment(ctx);

        // The produced variables of the this operator are missable because of the left outer semantics.
        Object outVarType, outPositionalVarType = null;
        Object t = env.getType(expression.getValue());
        IMissableTypeComputer tc = ctx.getMissableTypeComputer();
        if (missingValue.isMissing()) {
            outVarType = tc.makeMissableType(t);
            if (positionalVariable != null) {
                outPositionalVarType = tc.makeMissableType(positionalVariableType);
            }
        } else if (missingValue.isNull()) {
            outVarType = tc.makeNullableType(t);
            if (positionalVariable != null) {
                outPositionalVarType = tc.makeNullableType(positionalVariableType);
            }
        } else {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, getSourceLocation(), "");
        }

        env.setVarType(variables.get(0), outVarType);
        if (positionalVariable != null) {
            env.setVarType(positionalVariable, outPositionalVarType);
        }

        return env;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.LEFT_OUTER_UNNEST;
    }

    private static IAlgebricksConstantValue validateMissingValue(IAlgebricksConstantValue value) {
        if (value == null) {
            throw new NullPointerException();
        } else if (value.isMissing()) {
            return ConstantExpression.MISSING.getValue();
        } else if (value.isNull()) {
            return ConstantExpression.NULL.getValue();
        } else {
            throw new IllegalArgumentException(String.valueOf(value));
        }
    }
}
