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

package org.apache.hyracks.dataflow.std.parallel.base;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * @author michael
 */
public class DefaultSamplingWriter extends AbstractSamplingWriter {

    /**
     * @param ctx
     * @param sampleFields
     * @param sampleBasis
     * @param comparators
     * @param inRecordDesc
     * @param outRecordDesc
     * @param writer
     * @param outputPartial
     * @throws HyracksDataException
     */
    public DefaultSamplingWriter(IHyracksTaskContext ctx, int[] sampleFields, int sampleBasis,
            IBinaryComparator[] comparators, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc,
            IFrameWriter writer, boolean outputPartial) throws HyracksDataException {
        super(ctx, sampleFields, sampleBasis, comparators, inRecordDesc, outRecordDesc, writer, outputPartial);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param ctx
     * @param sampleFields
     * @param sampleBasis
     * @param comparators
     * @param inRecordDesc
     * @param outRecordDesc
     * @param writer
     * @throws HyracksDataException
     */
    public DefaultSamplingWriter(IHyracksTaskContext ctx, int[] sampleFields, int sampleBasis,
            IBinaryComparator[] comparators, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc,
            IFrameWriter writer) throws HyracksDataException {
        super(ctx, sampleFields, sampleBasis, comparators, inRecordDesc, outRecordDesc, writer);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void close() throws HyracksDataException {
        if (!isFailed && !isFirst) {
            assert (copyFrameAccessor.getTupleCount() > 0);
            writeOutput(copyFrameAccessor, copyFrameAccessor.getTupleCount() - 1);
        }
        super.close();
    }
}
