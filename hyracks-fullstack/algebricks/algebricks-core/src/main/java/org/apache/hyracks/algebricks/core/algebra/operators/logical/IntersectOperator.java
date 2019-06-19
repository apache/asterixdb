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
import java.util.stream.Collectors;

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

    private final List<List<LogicalVariable>> inputVars;
    private final List<List<LogicalVariable>> compareVars;
    private final List<LogicalVariable> outputVars;
    private List<List<LogicalVariable>> extraVars;

    public IntersectOperator(List<LogicalVariable> outputVars, List<List<LogicalVariable>> compareVars)
            throws AlgebricksException {
        this(outputVars, compareVars,
                compareVars.stream().map(vars -> new ArrayList<LogicalVariable>()).collect(Collectors.toList()));
    }

    public IntersectOperator(List<LogicalVariable> outputVars, List<List<LogicalVariable>> compareVars,
            List<List<LogicalVariable>> extraVars) throws AlgebricksException {
        int numCompareFields = compareVars.get(0).size();
        if (compareVars.stream().anyMatch(vlist -> vlist.size() != numCompareFields)) {
            throw AlgebricksException.create(ErrorCode.INVALID_INPUT_PARAMETER);
        }
        int numExtraFields = extraVars.get(0).size();
        if (extraVars.stream().anyMatch(vlist -> vlist.size() != numExtraFields)) {
            throw AlgebricksException.create(ErrorCode.INVALID_INPUT_PARAMETER);
        }
        if (outputVars.size() != numCompareFields + numExtraFields) {
            throw AlgebricksException.create(ErrorCode.INVALID_INPUT_PARAMETER);
        }

        this.outputVars = new ArrayList<>(outputVars);
        this.compareVars = new ArrayList<>(compareVars);
        this.inputVars = new ArrayList<>(compareVars.size());
        for (List<LogicalVariable> vars : compareVars) {
            this.inputVars.add(new ArrayList<>(vars));
        }
        for (int i = 0; i < extraVars.size(); i++) {
            this.inputVars.get(i).addAll(extraVars.get(i));
        }
        this.extraVars = extraVars;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.INTERSECT;
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        schema = outputVars;
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
        return new FilteredVariablePropagationPolicy(outputVars);
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment typeEnv = ctx.getOutputTypeEnvironment(inputs.get(0).getValue());

        List<LogicalVariable> compareVars0 = compareVars.get(0);
        for (int i = 1; i < inputs.size(); i++) {
            checkTypeConsistency(typeEnv, compareVars0, ctx.getOutputTypeEnvironment(inputs.get(i).getValue()),
                    compareVars.get(i));
        }

        IVariableTypeEnvironment env =
                new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMetadataProvider());
        int i = 0;
        for (; i < compareVars0.size(); i++) {
            env.setVarType(outputVars.get(i), typeEnv.getVarType(compareVars0.get(i)));
        }
        if (extraVars != null) {
            List<LogicalVariable> extraVars0 = extraVars.get(0);
            for (int k = 0; k < extraVars0.size(); k++) {
                env.setVarType(outputVars.get(i + k), typeEnv.getVarType(extraVars0.get(k)));
            }
        }
        return env;
    }

    public List<LogicalVariable> getOutputVars() {
        return outputVars;
    }

    public int getNumInput() {
        return compareVars.size();
    }

    public List<LogicalVariable> getCompareVariables(int inputIndex) {
        return compareVars.get(inputIndex);
    }

    public List<List<LogicalVariable>> getExtraVariables() {
        return extraVars;
    }

    public List<LogicalVariable> getInputVariables(int inputIndex) {
        return this.inputVars.get(inputIndex);
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
