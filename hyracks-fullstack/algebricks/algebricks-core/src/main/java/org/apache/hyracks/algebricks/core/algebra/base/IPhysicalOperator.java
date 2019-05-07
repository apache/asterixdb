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
package org.apache.hyracks.algebricks.core.algebra.base;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;

public interface IPhysicalOperator {

    public PhysicalOperatorTag getOperatorTag();

    /**
     * @param op
     *            the logical operator this physical operator annotates
     * @param reqdByParent
     *            parent's requirements, which are not enforced for now, as we
     *            only explore one plan
     * @param context
     *            the optimization context
     * @return for each child, one vector of required physical properties
     */
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) throws AlgebricksException;

    /**
     * @return the physical properties that this operator delivers, based on
     *         what its children deliver
     */
    public IPhysicalPropertiesVector getDeliveredProperties();

    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException;

    public LocalMemoryRequirements getLocalMemoryRequirements();

    public void createLocalMemoryRequirements(ILogicalOperator op);

    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException;

    public void disableJobGenBelowMe();

    public boolean isJobGenDisabledBelowMe();

    public boolean isMicroOperator();

    public void setHostQueryContext(Object context);

    public Object getHostQueryContext();

    /**
     * @return labels (0 or 1) for each input and output indicating the dependency between them.
     *         The edges labeled as 1 must wait for the edges with label 0.
     */
    public Pair<int[], int[]> getInputOutputDependencyLabels(ILogicalOperator op);

    /*
     * This is needed to have a kind of cost based decision on whether to merge the shared subplans and materialize
     * the result. If the subgraph whose result we would like to materialize has an operator that is computationally
     * expensive, we assume it is cheaper to materialize the result of this subgraph and read from the file rather
     * than recomputing it.
     */
    public boolean expensiveThanMaterialization();
}
