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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

/**
 * @author Nicola
 */
public abstract class AbstractAssignOperator extends AbstractLogicalOperator {
    protected final List<LogicalVariable> variables;
    protected final List<Mutable<ILogicalExpression>> expressions;

    public AbstractAssignOperator() {
        this.variables = new ArrayList<LogicalVariable>();
        this.expressions = new ArrayList<Mutable<ILogicalExpression>>();
    }

    public AbstractAssignOperator(List<LogicalVariable> variables, List<Mutable<ILogicalExpression>> expressions) {
        this.variables = variables;
        this.expressions = expressions;
    }

    public List<LogicalVariable> getVariables() {
        return variables;
    }

    public List<Mutable<ILogicalExpression>> getExpressions() {
        return expressions;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
        schema.addAll(inputs.get(0).getValue().getSchema());
        schema.addAll(variables);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean modif = false;
        for (int i = 0; i < expressions.size(); i++) {
            if (visitor.transform(expressions.get(i))) {
                modif = true;
            }
        }
        return modif;
    }

    protected VariablePropagationPolicy createVariablePropagationPolicy(boolean propagateInputVars) {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources) {
                if (propagateInputVars) {
                    target.addAllVariables(sources[0]);
                }
                for (LogicalVariable v : variables) {
                    target.addVariable(v);
                }
            }
        };
    }
}
