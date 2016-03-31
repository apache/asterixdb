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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class EmptyTupleSourceOperator extends AbstractLogicalOperator {

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.EMPTYTUPLESOURCE;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.NONE;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) {
        // do nothing
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitEmptyTupleSourceOperator(this, arg);
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(final ITypingContext ctx) throws AlgebricksException {
        return new IVariableTypeEnvironment() {

            @Override
            public void setVarType(LogicalVariable var, Object type) {
                throw new IllegalStateException();
            }

            @Override
            public Object getVarType(LogicalVariable var) throws AlgebricksException {
                return null;
            }

            @Override
            public Object getType(ILogicalExpression expr) throws AlgebricksException {
                return ctx.getExpressionTypeComputer().getType(expr, ctx.getMetadataProvider(), this);
            }

            @Override
            public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariables,
                    List<List<LogicalVariable>> correlatedNullableVariableLists) throws AlgebricksException {
                return null;
            }

            @Override
            public boolean substituteProducedVariable(LogicalVariable v1, LogicalVariable v2)
                    throws AlgebricksException {
                return false;
            }
        };
    }

}
