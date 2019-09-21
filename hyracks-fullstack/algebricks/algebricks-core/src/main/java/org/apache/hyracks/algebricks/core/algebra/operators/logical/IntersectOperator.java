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
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.ListUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.FilteredVariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.NonPropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.api.exceptions.ErrorCode;

public class IntersectOperator extends AbstractLogicalOperator {

    private final List<LogicalVariable> outputCompareVars;
    private final List<List<LogicalVariable>> inputCompareVars;

    private final List<LogicalVariable> outputExtraVars;
    private final List<List<LogicalVariable>> inputExtraVars;

    public IntersectOperator(List<LogicalVariable> outputCompareVars, List<List<LogicalVariable>> inputCompareVars)
            throws AlgebricksException {
        this(outputCompareVars, Collections.emptyList(), inputCompareVars, Collections.emptyList());
    }

    public IntersectOperator(List<LogicalVariable> outputCompareVars, List<LogicalVariable> outputExtraVars,
            List<List<LogicalVariable>> inputCompareVars, List<List<LogicalVariable>> inputExtraVars)
            throws AlgebricksException {
        int numCompareVars = outputCompareVars.size();
        for (List<LogicalVariable> vars : inputCompareVars) {
            if (vars.size() != numCompareVars) {
                throw AlgebricksException.create(ErrorCode.INVALID_INPUT_PARAMETER);
            }
        }
        if (outputExtraVars == null || outputExtraVars.isEmpty()) {
            if (inputExtraVars != null && !inputExtraVars.isEmpty()) {
                throw AlgebricksException.create(ErrorCode.INVALID_INPUT_PARAMETER);
            }
        } else {
            if (inputExtraVars == null || inputExtraVars.isEmpty()) {
                throw AlgebricksException.create(ErrorCode.INVALID_INPUT_PARAMETER);
            }
            int numExtraVars = outputExtraVars.size();
            for (List<LogicalVariable> vars : inputExtraVars) {
                if (vars.size() != numExtraVars) {
                    throw AlgebricksException.create(ErrorCode.INVALID_INPUT_PARAMETER);
                }
            }
        }

        this.outputCompareVars = new ArrayList<>(outputCompareVars);
        this.inputCompareVars = new ArrayList<>(inputCompareVars);
        this.outputExtraVars = new ArrayList<>();
        if (outputExtraVars != null) {
            this.outputExtraVars.addAll(outputExtraVars);
        }
        this.inputExtraVars = new ArrayList<>();
        if (inputExtraVars != null) {
            this.inputExtraVars.addAll(inputExtraVars);
        }
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.INTERSECT;
    }

    @Override
    public void recomputeSchema() {
        schema = concatOutputVariables();
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform)
            throws AlgebricksException {
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitIntersectOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new FilteredVariablePropagationPolicy(concatOutputVariables());
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment typeEnv = ctx.getOutputTypeEnvironment(inputs.get(0).getValue());

        List<LogicalVariable> inputCompareVars0 = inputCompareVars.get(0);
        for (int i = 1, n = inputs.size(); i < n; i++) {
            checkTypeConsistency(typeEnv, inputCompareVars0, ctx.getOutputTypeEnvironment(inputs.get(i).getValue()),
                    inputCompareVars.get(i));
        }

        IVariableTypeEnvironment env =
                new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMetadataProvider());
        for (int i = 0, n = outputCompareVars.size(); i < n; i++) {
            env.setVarType(outputCompareVars.get(i), typeEnv.getVarType(inputCompareVars0.get(i)));
        }
        if (hasExtraVariables()) {
            List<LogicalVariable> inputExtraVars0 = inputExtraVars.get(0);
            for (int i = 0, n = outputExtraVars.size(); i < n; i++) {
                env.setVarType(outputExtraVars.get(i), typeEnv.getVarType(inputExtraVars0.get(i)));
            }
        }
        return env;
    }

    public int getNumInput() {
        return inputCompareVars.size();
    }

    public boolean hasExtraVariables() {
        return !outputExtraVars.isEmpty();
    }

    public List<LogicalVariable> getInputCompareVariables(int inputIndex) {
        return inputCompareVars.get(inputIndex);
    }

    public List<LogicalVariable> getInputExtraVariables(int inputIndex) {
        return inputExtraVars.get(inputIndex);
    }

    public List<List<LogicalVariable>> getAllInputsCompareVariables() {
        return inputCompareVars;
    }

    public List<List<LogicalVariable>> getAllInputsExtraVariables() {
        return inputExtraVars;
    }

    public List<LogicalVariable> getOutputCompareVariables() {
        return outputCompareVars;
    }

    public List<LogicalVariable> getOutputExtraVariables() {
        return outputExtraVars;
    }

    private List<LogicalVariable> concatOutputVariables() {
        return ListUtils.union(outputCompareVars, outputExtraVars);
    }

    private void checkTypeConsistency(IVariableTypeEnvironment expected, List<LogicalVariable> expectedVariables,
            IVariableTypeEnvironment actual, List<LogicalVariable> actualVariables) throws AlgebricksException {
        for (int i = 0; i < expectedVariables.size(); i++) {
            Object expectedType = expected.getVarType(expectedVariables.get(i));
            Object actualType = actual.getVarType(actualVariables.get(i));
            if (!expectedType.equals(actualType)) {
                if (AlgebricksConfig.ALGEBRICKS_LOGGER.isWarnEnabled()) {
                    AlgebricksConfig.ALGEBRICKS_LOGGER
                            .warn("Type of two variables are not equal." + expectedVariables.get(i) + " is of type: "
                                    + expectedType + actualVariables.get(i) + " is of type: " + actualType);
                }
            }
        }
    }
}
