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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.OpRefTypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class SinkOperator extends AbstractLogicalOperator {

    @Override
    public void recomputeSchema() throws AlgebricksException {
        schema = new ArrayList<LogicalVariable>();
        for (int i = 0; i < inputs.size(); i++) {
            for (LogicalVariable v : inputs.get(i).getValue().getSchema()) {
                if (!schema.contains(v))
                    schema.add(v);
            }

        }
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform)
            throws AlgebricksException {
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitSinkOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        ITypeEnvPointer[] envPointers = new ITypeEnvPointer[inputs.size()];
        for (int i = 0; i < inputs.size(); i++) {
            envPointers[i] = new OpRefTypeEnvPointer(inputs.get(i), ctx);
        }
        PropagatingTypeEnvironment env = new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(),
                ctx.getMissableTypeComputer(), ctx.getMetadataProvider(), TypePropagationPolicy.ALL, envPointers);
        return env;

    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.SINK;
    }

}
