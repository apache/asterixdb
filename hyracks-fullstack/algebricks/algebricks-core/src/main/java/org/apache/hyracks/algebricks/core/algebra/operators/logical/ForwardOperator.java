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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.OpRefTypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * Forward operator is used to forward data to different NCs based on the side data activity that is computed
 * dynamically by doing a pass over the data itself to infer the range map. The operator takes two inputs:
 * 1. Tuples/data (at index 0). The data is forwarded to the range-based connector which routes it to the target NC.
 * 2. Side Activity (at index 1). The output will be stored in Hyracks context, and the connector will pick it up.
 * Forward operator will receive the range map when it is broadcast by the operator generating the side activity output
 * after which the forward operator will start forwarding the data.
 */
public class ForwardOperator extends AbstractLogicalOperator {

    private final String sideDataKey;
    private final Mutable<ILogicalExpression> sideDataExpression;

    public ForwardOperator(String sideDataKey, Mutable<ILogicalExpression> sideDataExpression) {
        super();
        this.sideDataKey = sideDataKey;
        this.sideDataExpression = sideDataExpression;
    }

    public String getSideDataKey() {
        return sideDataKey;
    }

    public Mutable<ILogicalExpression> getSideDataExpression() {
        return sideDataExpression;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.FORWARD;
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        // schema is equal to the schema of the data source at idx 0
        setSchema(inputs.get(0).getValue().getSchema());
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return visitor.transform(sideDataExpression);
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitForwardOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {

            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                // propagate the variables of the data source at idx 0
                if (sources.length > 0) {
                    target.addAllVariables(sources[0]);
                }
            }
        };
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        // propagate the type environment of the data source at idx 0
        ITypeEnvPointer[] envPointers = new ITypeEnvPointer[] { new OpRefTypeEnvPointer(inputs.get(0), ctx) };
        return new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMissableTypeComputer(),
                ctx.getMetadataProvider(), TypePropagationPolicy.ALL, envPointers);
    }
}
