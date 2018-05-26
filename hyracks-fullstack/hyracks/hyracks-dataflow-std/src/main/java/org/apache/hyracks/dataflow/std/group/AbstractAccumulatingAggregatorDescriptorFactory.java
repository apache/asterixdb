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
package org.apache.hyracks.dataflow.std.group;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractAccumulatingAggregatorDescriptorFactory extends AbstractAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory#createAggregator(org.apache.hyracks.api.context.IHyracksTaskContext, org.apache.hyracks.api.dataflow.value.RecordDescriptor, org.apache.hyracks.api.dataflow.value.RecordDescriptor, int[], int[], org.apache.hyracks.api.comm.IFrameWriter)
     */
    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults, IFrameWriter writer,
            long memoryBudget) throws HyracksDataException {
        return createAggregator(ctx, inRecordDescriptor, outRecordDescriptor, keyFields, keyFieldsInPartialResults,
                memoryBudget);
    }

    abstract protected IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, int[] keyFields,
            final int[] keyFieldsInPartialResults, long memoryBudget) throws HyracksDataException;

}
