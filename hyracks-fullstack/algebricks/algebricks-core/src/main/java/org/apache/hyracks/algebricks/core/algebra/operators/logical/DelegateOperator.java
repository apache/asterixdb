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
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * @author rico
 */
public class DelegateOperator extends AbstractLogicalOperator {

    private IOperatorDelegate delegate;

    public DelegateOperator(IOperatorDelegate delegate) {
        super();
        if (delegate == null) {
            throw new IllegalArgumentException("delegate cannot be null!");
        }
        this.delegate = delegate;
        setExecutionMode(delegate.getExecutionMode());
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        schema = new ArrayList<LogicalVariable>(inputs.get(0).getValue().getSchema());
        delegate.setSchema(schema);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform)
            throws AlgebricksException {
        return delegate.acceptExpressionTransform(transform);
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitDelegateOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return this.delegate.isMap();
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return this.createPropagatingAllInputsTypeEnvironment(ctx);
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.DELEGATE_OPERATOR;
    }

    public IOperatorDelegate getNewInstanceOfDelegateOperator() {
        return delegate.newInstance();
    }

    @Override
    public List<LogicalVariable> getSchema() {
        return this.schema;
    }

    @Override
    public ExecutionMode getExecutionMode() {
        return delegate.getExecutionMode();
    }

    @Override
    public void setExecutionMode(ExecutionMode mode) {
        delegate.setExecutionMode(mode);
    }

    @Override
    public IPhysicalOperator getPhysicalOperator() {
        return delegate.getPhysicalOperator();
    }

    @Override
    public IVariableTypeEnvironment computeInputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return this.createPropagatingAllInputsTypeEnvironment(ctx);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    public IOperatorDelegate getDelegate() {
        return delegate;
    }

}
