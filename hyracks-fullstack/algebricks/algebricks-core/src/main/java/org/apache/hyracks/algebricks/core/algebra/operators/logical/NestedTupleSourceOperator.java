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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class NestedTupleSourceOperator extends AbstractLogicalOperator {
    private Mutable<ILogicalOperator> dataSourceReference;

    public NestedTupleSourceOperator(Mutable<ILogicalOperator> dataSourceReference) {
        this.dataSourceReference = dataSourceReference;
    }

    public ILogicalOperator getSourceOperator() {
        return dataSourceReference.getValue().getInputs().get(0).getValue();
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.NESTEDTUPLESOURCE;
    }

    public Mutable<ILogicalOperator> getDataSourceReference() {
        return dataSourceReference;
    }

    public void setDataSourceReference(Mutable<ILogicalOperator> dataSourceReference) {
        this.dataSourceReference = dataSourceReference;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
        ILogicalOperator topOp = dataSourceReference.getValue();
        for (Mutable<ILogicalOperator> i : topOp.getInputs()) {
            List<LogicalVariable> inputSchema = i.getValue().getSchema();
            if (inputSchema != null) {
                schema.addAll(inputSchema);
            }
        }
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) {
        // do nothing
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitNestedTupleSourceOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(final ITypingContext ctx) throws AlgebricksException {
        ITypeEnvPointer[] p = new ITypeEnvPointer[1];
        p[0] = new ITypeEnvPointer() {

            @Override
            public IVariableTypeEnvironment getTypeEnv() {
                ILogicalOperator op = dataSourceReference.getValue().getInputs().get(0).getValue();
                return ctx.getOutputTypeEnvironment(op);
            }
        };
        return new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMissableTypeComputer(),
                ctx.getMetadataProvider(), TypePropagationPolicy.ALL, p);
    }

    @Override
    public IVariableTypeEnvironment computeInputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return computeOutputTypeEnvironment(ctx);
    }

}
