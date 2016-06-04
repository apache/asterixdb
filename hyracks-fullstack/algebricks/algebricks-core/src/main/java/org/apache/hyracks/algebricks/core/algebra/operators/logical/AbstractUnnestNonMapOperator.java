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
import org.apache.hyracks.algebricks.runtime.base.IUnnestingPositionWriter;

public abstract class AbstractUnnestNonMapOperator extends AbstractUnnestOperator {

    protected LogicalVariable positionalVariable;

    /**
     * Specify the writer of the positional variable
     */
    protected IUnnestingPositionWriter positionWriter;

    /**
     * Specify the type of the positional variable
     */
    protected Object positionalVariableType;

    public AbstractUnnestNonMapOperator(LogicalVariable variable, Mutable<ILogicalExpression> expression) {
        super(makeSingletonList(variable), expression);
    }

    public AbstractUnnestNonMapOperator(LogicalVariable variable, Mutable<ILogicalExpression> expression,
            LogicalVariable positionalVariable, Object positionalVariableType,
            IUnnestingPositionWriter positionWriter) {
        this(variable, expression);
        this.setPositionalVariable(positionalVariable);
        this.setPositionalVariableType(positionalVariableType);
        this.setPositionWriter(positionWriter);
    }

    public LogicalVariable getVariable() {
        return variables.get(0);
    }

    public void setPositionalVariable(LogicalVariable positionalVariable) {
        this.positionalVariable = positionalVariable;
    }

    public LogicalVariable getPositionalVariable() {
        return positionalVariable;
    }

    public void setPositionWriter(IUnnestingPositionWriter positionWriter) {
        this.positionWriter = positionWriter;
    }

    public IUnnestingPositionWriter getPositionWriter() {
        return positionalVariable != null ? positionWriter : null;
    }

    public void setPositionalVariableType(Object positionalVariableType) {
        this.positionalVariableType = positionalVariableType;
    }

    public Object getPositionalVariableType() {
        return positionalVariableType;
    }

    protected static <E> List<E> makeSingletonList(E item) {
        List<E> array = new ArrayList<>(1);
        array.add(item);
        return array;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {

            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                if (sources.length > 0) {
                    target.addAllVariables(sources[0]);
                }
                for (LogicalVariable v : variables) {
                    target.addVariable(v);
                }
                if (positionalVariable != null) {
                    target.addVariable(positionalVariable);
                }
            }
        };
    }

}
