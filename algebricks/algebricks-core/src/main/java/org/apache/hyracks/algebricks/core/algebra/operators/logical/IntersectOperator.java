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

public class IntersectOperator extends AbstractLogicalOperator {

    private final List<List<LogicalVariable>> inputVars;
    private final List<LogicalVariable> outputVars;

    public IntersectOperator(List<LogicalVariable> outputVars, List<List<LogicalVariable>> inputVars)
            throws AlgebricksException {
        if (outputVars.size() != inputVars.get(0).size()) {
            throw new AlgebricksException("The number of output variables is different with the input variable number");
        }
        if (inputVars.stream().anyMatch(vlist -> vlist.size() != outputVars.size())) {
            throw new AlgebricksException("The schemas of input variables are not consistent");
        }
        this.outputVars = outputVars;
        this.inputVars = inputVars;
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

        for (int i = 1; i < inputs.size(); i++) {
            checkTypeConsistency(typeEnv, inputVars.get(0), ctx.getOutputTypeEnvironment(inputs.get(i).getValue()),
                    inputVars.get(i));
        }

        IVariableTypeEnvironment env = new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(),
                ctx.getMetadataProvider());
        for (int i = 0; i < outputVars.size(); i++) {
            env.setVarType(outputVars.get(i), typeEnv.getVarType(inputVars.get(0).get(i)));
        }
        return typeEnv;
    }

    public List<LogicalVariable> getOutputVars() {
        return outputVars;
    }

    public int getNumInput() {
        return inputVars.size();
    }

    public List<LogicalVariable> getInputVariables(int inputIndex) {
        return inputVars.get(inputIndex);
    }

    private void checkTypeConsistency(IVariableTypeEnvironment expected, List<LogicalVariable> expectedVariables,
            IVariableTypeEnvironment actual, List<LogicalVariable> actualVariables) throws AlgebricksException {
        for (int i = 0; i < expectedVariables.size(); i++) {
            Object expectedType = expected.getVarType(expectedVariables.get(i));
            Object actualType = actual.getVarType(actualVariables.get(i));
            if (!expectedType.equals(actualType)) {
                AlgebricksConfig.ALGEBRICKS_LOGGER
                        .warning("Type of two variables are not equal." + expectedVariables.get(i) + " is of type: "
                                + expectedType + actualVariables.get(i) + " is of type: " + actualType);
            }
        }
    }

}
