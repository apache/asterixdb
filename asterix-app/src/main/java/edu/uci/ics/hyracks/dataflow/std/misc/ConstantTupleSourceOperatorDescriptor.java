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
package edu.uci.ics.hyracks.dataflow.std.misc;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class ConstantTupleSourceOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private int[] fieldSlots;
    private byte[] tupleData;
    private int tupleSize;

    public ConstantTupleSourceOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc, int[] fieldSlots,
            byte[] tupleData, int tupleSize) {
        super(spec, 0, 1);
        this.tupleData = tupleData;
        this.fieldSlots = fieldSlots;
        this.tupleSize = tupleSize;
        recordDescriptors[0] = recDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new ConstantTupleSourceOperatorNodePushable(ctx, fieldSlots, tupleData, tupleSize);
    }
}