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
import org.apache.hyracks.algebricks.core.algebra.base.DefaultProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * Left-outer-unnest-map is similar to the unnest-map operator. The only
 * difference is that this operator represents left-outer semantics, meaning
 * that it generates null values for non-matching tuples. It also propagates all
 * input variables. This may be used only in a left-outer-join case.
 */
public class LeftOuterUnnestMapOperator extends AbstractUnnestMapOperator {

    private IAlgebricksConstantValue missingValue;
    private IProjectionFiltrationInfo projectionFiltrationInfo;

    public LeftOuterUnnestMapOperator(List<LogicalVariable> variables, Mutable<ILogicalExpression> expression,
            List<Object> variableTypes, IAlgebricksConstantValue missingValue) {
        this(variables, expression, variableTypes, missingValue, DefaultProjectionFiltrationInfo.INSTANCE);
    }

    public LeftOuterUnnestMapOperator(List<LogicalVariable> variables, Mutable<ILogicalExpression> expression,
            List<Object> variableTypes, IAlgebricksConstantValue missingValue,
            IProjectionFiltrationInfo projectionFiltrationInfo) {
        // propagateInput is always set to true for this operator.
        super(variables, expression, variableTypes, true);
        setMissingValue(missingValue);
        setProjectionFiltrationInfo(projectionFiltrationInfo);
    }

    public IAlgebricksConstantValue getMissingValue() {
        return missingValue;
    }

    public void setMissingValue(IAlgebricksConstantValue missingValue) {
        this.missingValue = validateMissingValue(missingValue);
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

        // The produced variables of the this operator are missable (or nullable) because of the left outer semantics.
        for (int i = 0; i < variables.size(); i++) {
            Object varType = variableTypes.get(i);
            Object outVarType;
            if (missingValue.isMissing()) {
                outVarType = ctx.getMissableTypeComputer().makeMissableType(varType);
            } else if (missingValue.isNull()) {
                outVarType = ctx.getMissableTypeComputer().makeNullableType(varType);
            } else {
                throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, getSourceLocation(), "");
            }
            env.setVarType(variables.get(i), outVarType);
        }

        return env;
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

    public void setProjectionFiltrationInfo(IProjectionFiltrationInfo projectionFiltrationInfo) {
        this.projectionFiltrationInfo =
                projectionFiltrationInfo == null ? DefaultProjectionFiltrationInfo.INSTANCE : projectionFiltrationInfo;
    }

    public IProjectionFiltrationInfo getProjectionFiltrationInfo() {
        return projectionFiltrationInfo;
    }
}
