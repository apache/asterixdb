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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.FilteredVariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class ProjectOperator extends AbstractLogicalOperator {

    private final List<LogicalVariable> variables;

    public ProjectOperator(List<LogicalVariable> variables) {
        this.variables = variables;
    }

    public ProjectOperator(LogicalVariable v) {
        this.variables = new ArrayList<LogicalVariable>(1);
        variables.add(v);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) {
        // Do nothing
        return false;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.PROJECT;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>(getVariables());
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new FilteredVariablePropagationPolicy(getVariables());
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitProjectOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return true;
    }

    public List<LogicalVariable> getVariables() {
        return variables;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }
}
