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
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;

public abstract class AbstractUnnestMapOperator extends AbstractUnnestOperator {

    protected final Mutable<ILogicalExpression> expression;
    protected final List<Object> variableTypes;
    protected boolean propagateInput;
    protected List<Mutable<ILogicalExpression>> additionalFilteringExpressions;
    protected List<LogicalVariable> minFilterVars;
    protected List<LogicalVariable> maxFilterVars;

    protected boolean propagateIndexFilter;
    // Used when the result of a searchCallBack.proceed() is required afterwards.
    protected boolean generateSearchCallBackProceedResultVar;

    public AbstractUnnestMapOperator(List<LogicalVariable> variables, Mutable<ILogicalExpression> expression,
            List<Object> variableTypes, boolean propagateInput) {
        super(variables, expression);
        this.expression = expression;
        this.variableTypes = variableTypes;
        this.propagateInput = propagateInput;
        this.propagateIndexFilter = false;
        this.generateSearchCallBackProceedResultVar = false;
    }

    @Override
    public List<LogicalVariable> getScanVariables() {
        // An additional variable - generateSearchCallBackProceedResultVar should not be returned.
        int excludeVarCount = 0;
        if (propagateIndexFilter) {
            excludeVarCount += 2;
        }
        if (generateSearchCallBackProceedResultVar) {
            excludeVarCount++;
        }
        return excludeVarCount > 0 ? variables.subList(0, variables.size() - excludeVarCount) : variables;
    }

    public List<Object> getVariableTypes() {
        return variableTypes;
    }

    /**
     * If propagateInput is true, then propagates the input variables.
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

    public boolean propagatesInput() {
        return propagateInput;
    }

    public void setPropagatesInput(boolean propagateInput) {
        this.propagateInput = propagateInput;
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

    public void markPropagageIndexFilter() {
        this.propagateIndexFilter = true;
    }

    public boolean propagateIndexFilter() {
        return this.propagateIndexFilter;
    }

    public LogicalVariable getPropagateIndexMinFilterVar() {
        if (propagateIndexFilter) {
            return variables.get(variables.size() - 2);
        } else {
            return null;
        }
    }

    public LogicalVariable getPropagateIndexMaxFilterVar() {
        if (propagateIndexFilter) {
            return variables.get(variables.size() - 1);
        } else {
            return null;
        }
    }

    /**
     * Sets the variable to tell whether the result of a searchCallBack.proceed() is required.
     * If this variable is set to true, the last variable in the variables list should contain
     * the result of a searchCallBack.proceed().
     */
    public void setGenerateCallBackProceedResultVar(boolean generateCallBackProceedResultVar) {
        this.generateSearchCallBackProceedResultVar = generateCallBackProceedResultVar;
    }

    public boolean getGenerateCallBackProceedResultVar() {
        return this.generateSearchCallBackProceedResultVar;
    }

}
