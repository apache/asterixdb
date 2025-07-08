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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.sort.AbstractSorterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.TopKSorterOperatorDescriptor;

/**
 * This will always be attached to an {@link OrderOperator} logical operator.
 */

public class StableSortPOperator extends AbstractStableSortPOperator {

    private final int topK;

    public StableSortPOperator() {
        this(-1);
    }

    public StableSortPOperator(int topK) {
        this.topK = topK;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.STABLE_SORT;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        SortSetupData sortSetupData = setupSortOperator(context, op, opSchema, sortColumns, localMemoryRequirements);

        AbstractSorterOperatorDescriptor sortOpDesc;
        // topK == -1 means that a topK value is not provided.
        if (topK == -1) {
            sortOpDesc = new ExternalSortOperatorDescriptor(spec, sortSetupData.maxNumberOfFrames,
                    sortSetupData.sortFields, sortSetupData.nkcf, sortSetupData.comps, sortSetupData.recDescriptor);
        } else {
            // Since topK value is provided, topK optimization is possible.
            // We call topKSorter instead of calling ExternalSortOperator.
            sortOpDesc = new TopKSorterOperatorDescriptor(spec, sortSetupData.maxNumberOfFrames, topK,
                    sortSetupData.sortFields, sortSetupData.nkcf, sortSetupData.comps, sortSetupData.recDescriptor);
        }
        sortOpDesc.setSourceLocation(op.getSourceLocation());
        contributeOpDesc(builder, (AbstractLogicalOperator) op, sortOpDesc);
        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();
        out.append(getOperatorTag());
        if (topK != -1) {
            out.append(" [topK: ").append(topK).append(']');
        }
        if (orderProp != null) {
            out.append(' ').append(orderProp);
        }
        return out.toString();
    }
}
