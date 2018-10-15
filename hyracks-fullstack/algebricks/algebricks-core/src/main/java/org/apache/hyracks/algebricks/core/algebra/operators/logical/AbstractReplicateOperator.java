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
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

/**
 * Abstract class for two replication related operator - replicate and split
 * Replicate operator propagates all frames to all output branches.
 * That is, each tuple will be propagated to all output branches.
 * Split operator propagates each tuple in a frame to one output branch only.
 */
public abstract class AbstractReplicateOperator extends AbstractLogicalOperator {

    private int outputArity;
    private boolean[] outputMaterializationFlags;
    private List<Mutable<ILogicalOperator>> outputs;

    public AbstractReplicateOperator(int outputArity) {
        this.outputArity = outputArity;
        this.outputMaterializationFlags = new boolean[outputArity];
        this.outputs = new ArrayList<>();
    }

    public AbstractReplicateOperator(int outputArity, boolean[] outputMaterializationFlags) {
        this.outputArity = outputArity;
        this.outputMaterializationFlags = outputMaterializationFlags;
        this.outputs = new ArrayList<>();
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform)
            throws AlgebricksException {
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public boolean isMap() {
        return true;
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

    public void setOutputMaterializationFlags(boolean[] outputMaterializationFlags) {
        this.outputMaterializationFlags = outputMaterializationFlags;
    }

    public boolean[] getOutputMaterializationFlags() {
        return outputMaterializationFlags;
    }

    public List<Mutable<ILogicalOperator>> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<Pair<Mutable<ILogicalOperator>, Boolean>> newOutputs) {
        // shrinking or expanding num of outputs
        if (outputMaterializationFlags.length != newOutputs.size()) {
            outputMaterializationFlags = new boolean[newOutputs.size()];
        }
        outputs.clear();
        for (int i = 0; i < newOutputs.size(); i++) {
            outputs.add(newOutputs.get(i).first);
            outputMaterializationFlags[i] = newOutputs.get(i).second;
        }
        outputArity = newOutputs.size();
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

    public boolean isMaterialized(ILogicalOperator op) {
        for (int i = 0; i < outputs.size(); i++) {
            if (outputs.get(i).getValue() == op) {
                return outputMaterializationFlags[i];
            }
        }
        return false;
    }
}
