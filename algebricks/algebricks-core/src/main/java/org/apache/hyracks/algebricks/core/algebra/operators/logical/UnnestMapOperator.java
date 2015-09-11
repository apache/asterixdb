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
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.NonPropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * This operator may go away after we add indexes to Algebricks.
 */
public class UnnestMapOperator extends AbstractUnnestOperator {

    private final List<Object> variableTypes; // TODO: get rid of this and  deprecate UnnestMap
    private boolean propagateInput;

    private List<Mutable<ILogicalExpression>> additionalFilteringExpressions;
    private List<LogicalVariable> minFilterVars;
    private List<LogicalVariable> maxFilterVars;

    public UnnestMapOperator(List<LogicalVariable> variables, Mutable<ILogicalExpression> expression,
            List<Object> variableTypes, boolean propagateInput) {
        super(variables, expression);
        this.variableTypes = variableTypes;
        this.propagateInput = propagateInput;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.UNNEST_MAP;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitUnnestMapOperator(this, arg);
    }

    /**
     * UnnestMap doesn't propagate input variables, because currently it is only
     * used to search indexes. In the future, it would be nice to have the
     * choice to propagate input variables or not.
     */
    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                if (propagateInput) {
                    target.addAllVariables(sources[0]);
                }
                for (LogicalVariable v : variables) {
                    target.addVariable(v);
                }
            }
        };
    }

    public List<Object> getVariableTypes() {
        return variableTypes;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env = null;
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

    public boolean propagatesInput() {
        return propagateInput;
    }

    public List<LogicalVariable> getMinFilterVars() {
        return minFilterVars;
    }

    public void setMinFilterVars(List<LogicalVariable> minFilterVars) {
        this.minFilterVars = minFilterVars;
    }

    public List<LogicalVariable> getMaxFilterVars() {
        return maxFilterVars;
    }

    public void setMaxFilterVars(List<LogicalVariable> maxFilterVars) {
        this.maxFilterVars = maxFilterVars;
    }

    public void setAdditionalFilteringExpressions(List<Mutable<ILogicalExpression>> additionalFilteringExpressions) {
        this.additionalFilteringExpressions = additionalFilteringExpressions;
    }

    public List<Mutable<ILogicalExpression>> getAdditionalFilteringExpressions() {
        return additionalFilteringExpressions;
    }

    /*
    @Override
    public boolean isMap() {
        return !propagateInput;
    }
    */
}