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
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class ReplicateOperator extends AbstractLogicalOperator {

    private int outputArity = 2;
    private boolean[] outputMaterializationFlags = new boolean[outputArity];
    private List<Mutable<ILogicalOperator>> outputs;

    public ReplicateOperator(int outputArity) {
        this.outputArity = outputArity;
        this.outputMaterializationFlags = new boolean[outputArity];
        this.outputs = new ArrayList<Mutable<ILogicalOperator>>();
    }

    public ReplicateOperator(int outputArity, boolean[] outputMaterializationFlags) {
        this.outputArity = outputArity;
        this.outputMaterializationFlags = outputMaterializationFlags;
        this.outputs = new ArrayList<Mutable<ILogicalOperator>>();
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.REPLICATE;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitReplicateOperator(this, arg);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform) throws AlgebricksException {
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>(inputs.get(0).getValue().getSchema());
    }

    public void substituteVar(LogicalVariable v1, LogicalVariable v2) {
        // do nothing
    }

    public int getOutputArity() {
        return outputArity;
    }

    public int setOutputArity(int outputArity) {
        return this.outputArity = outputArity;
    }

    public void setOutputMaterializationFlags(boolean[] outputMaterializationFlags) {
        this.outputMaterializationFlags = outputMaterializationFlags;
    }

    public boolean[] getOutputMaterializationFlags() {
        return outputMaterializationFlags;
    }

    public List<Mutable<ILogicalOperator>> getOutputs() {
        return outputs;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }

    public boolean isBlocker() {
        for (boolean requiresMaterialization : outputMaterializationFlags) {
            if (requiresMaterialization) {
                return true;
            }
        }
        return false;
    }
}
