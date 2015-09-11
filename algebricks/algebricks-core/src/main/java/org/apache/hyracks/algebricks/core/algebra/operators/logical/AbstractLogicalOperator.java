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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.OpRefTypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;

public abstract class AbstractLogicalOperator implements ILogicalOperator {

    /*********************************************************************
     * UNPARTITIONED, the input data is not partitioned
     * PARTITIONED, the input data is partitioned, the operator is executed on
     * each partition and may receive input from other partitions (e.g. if it is
     * a join or an aggregate)
     * LOCAL, the input data is partitioned, the operator is executed on each
     * partition and only processes data from that partition
     */

    public static enum ExecutionMode {
        UNPARTITIONED,
        PARTITIONED,
        LOCAL
    }

    private AbstractLogicalOperator.ExecutionMode mode = AbstractLogicalOperator.ExecutionMode.UNPARTITIONED;
    protected IPhysicalOperator physicalOperator;
    private final Map<String, Object> annotations = new HashMap<String, Object>();
    private boolean bJobGenEnabled = true;

    final protected List<Mutable<ILogicalOperator>> inputs;
    // protected List<LogicalOperatorReference> outputs;
    protected List<LogicalVariable> schema;

    public AbstractLogicalOperator() {
        inputs = new ArrayList<Mutable<ILogicalOperator>>();
        // outputs = new ArrayList<LogicalOperatorReference>();
    }

    @Override
    public abstract LogicalOperatorTag getOperatorTag();

    public ExecutionMode getExecutionMode() {
        return mode;
    }

    public void setExecutionMode(ExecutionMode mode) {
        this.mode = mode;
    }

    @Override
    public List<LogicalVariable> getSchema() {
        return schema;
    }

    public void setPhysicalOperator(IPhysicalOperator physicalOp) {
        this.physicalOperator = physicalOp;
    }

    public IPhysicalOperator getPhysicalOperator() {
        return physicalOperator;
    }

    /**
     * @return for each child, one vector of required physical properties
     */

    @Override
    public final PhysicalRequirements getRequiredPhysicalPropertiesForChildren(
            IPhysicalPropertiesVector requiredProperties) {
        return physicalOperator.getRequiredPropertiesForChildren(this, requiredProperties);
    }

    /**
     * @return the physical properties that this operator delivers, based on
     *         what its children deliver
     */

    @Override
    public final IPhysicalPropertiesVector getDeliveredPhysicalProperties() {
        return physicalOperator.getDeliveredProperties();
    }

    @Override
    public final void computeDeliveredPhysicalProperties(IOptimizationContext context) throws AlgebricksException {
        physicalOperator.computeDeliveredProperties(this, context);
    }

    @Override
    public final List<Mutable<ILogicalOperator>> getInputs() {
        return inputs;
    }

    // @Override
    // public final List<LogicalOperatorReference> getOutputs() {
    // return outputs;
    // }

    @Override
    public final boolean hasInputs() {
        return !inputs.isEmpty();
    }

    public boolean hasNestedPlans() {
        return false;
    }

    @Override
    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    @Override
    public void removeAnnotation(String annotationName) {
        annotations.remove(annotationName);
    }

    @Override
    public final void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        if (bJobGenEnabled) {
            if (physicalOperator == null) {
                throw new AlgebricksException("Physical operator not set for operator: " + this);
            }
            physicalOperator.contributeRuntimeOperator(builder, context, this, propagatedSchema, inputSchemas,
                    outerPlanSchema);
        }
    }

    public void disableJobGen() {
        bJobGenEnabled = false;
    }

    public boolean isJobGenEnabled() {
        return bJobGenEnabled;
    }

    @Override
    public IVariableTypeEnvironment computeInputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }

    protected PropagatingTypeEnvironment createPropagatingAllInputsTypeEnvironment(ITypingContext ctx) {
        int n = inputs.size();
        ITypeEnvPointer[] envPointers = new ITypeEnvPointer[n];
        for (int i = 0; i < n; i++) {
            envPointers[i] = new OpRefTypeEnvPointer(inputs.get(i), ctx);
        }
        return new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getNullableTypeComputer(),
                ctx.getMetadataProvider(), TypePropagationPolicy.ALL, envPointers);
    }

    @Override
    public boolean requiresVariableReferenceExpressions() {
        return true;
    }
}
