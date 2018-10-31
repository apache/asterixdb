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
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * It corresponds to the Map operator in other algebras.
 *
 * @author Nicola
 */

public class AssignOperator extends AbstractAssignOperator {

    private LocalOrderProperty explicitOrderingProperty;

    public AssignOperator(List<LogicalVariable> vars, List<Mutable<ILogicalExpression>> exprs) {
        super(vars, exprs);
    }

    public AssignOperator(LogicalVariable var, Mutable<ILogicalExpression> expr) {
        this.variables.add(var);
        this.expressions.add(expr);
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.ASSIGN;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitAssignOperator(this, arg);
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return createVariablePropagationPolicy(true);
    }

    @Override
    public boolean isMap() {
        return true;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        PropagatingTypeEnvironment env = createPropagatingAllInputsTypeEnvironment(ctx);
        int n = variables.size();
        for (int i = 0; i < n; i++) {
            env.setVarType(variables.get(i), ctx.getExpressionTypeComputer().getType(expressions.get(i).getValue(),
                    ctx.getMetadataProvider(), env));
            if (expressions.get(i).getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                LogicalVariable var =
                        ((VariableReferenceExpression) expressions.get(i).getValue()).getVariableReference();
                for (List<LogicalVariable> list : env.getCorrelatedMissableVariableLists()) {
                    if (list.contains(var)) {
                        list.add(variables.get(i));
                    }
                }
            }
        }
        return env;
    }

    @Override
    public boolean requiresVariableReferenceExpressions() {
        return false;
    }

    public LocalOrderProperty getExplicitOrderingProperty() {
        return explicitOrderingProperty;
    }

    public void setExplicitOrderingProperty(LocalOrderProperty explicitOrderingProperty) {
        this.explicitOrderingProperty = explicitOrderingProperty;
    }
}
