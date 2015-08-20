/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.OrderColumn;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;

/**
 * This will always be attached to an {@link OrderOperator} logical operator.
 */

public class StableSortPOperator extends AbstractStableSortPOperator {

    private int maxNumberOfFrames;

    public StableSortPOperator(int maxNumberOfFrames) {
        super();
        this.maxNumberOfFrames = maxNumberOfFrames;
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
        RecordDescriptor recDescriptor = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);
        int n = sortColumns.length;
        int[] sortFields = new int[n];
        IBinaryComparatorFactory[] comps = new IBinaryComparatorFactory[n];

        INormalizedKeyComputerFactoryProvider nkcfProvider = context.getNormalizedKeyComputerFactoryProvider();
        INormalizedKeyComputerFactory nkcf = null;

        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        int i = 0;
        for (OrderColumn oc : sortColumns) {
            LogicalVariable var = oc.getColumn();
            sortFields[i] = opSchema.findVariable(var);
            Object type = env.getVarType(var);
            OrderKind order = oc.getOrder();
            if (i == 0 && nkcfProvider != null && type != null) {
                nkcf = nkcfProvider.getNormalizedKeyComputerFactory(type, order == OrderKind.ASC);
            }
            IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
            comps[i] = bcfp.getBinaryComparatorFactory(type, oc.getOrder() == OrderKind.ASC);
            i++;
        }

        ExternalSortOperatorDescriptor sortOpDesc = new ExternalSortOperatorDescriptor(spec, maxNumberOfFrames,
                sortFields, nkcf, comps, recDescriptor);
        contributeOpDesc(builder, (AbstractLogicalOperator) op, sortOpDesc);
        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }
}
