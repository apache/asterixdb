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
package org.apache.hyracks.dataflow.std.group.aggregators;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.std.group.AggregateState;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptor;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;

/**
 *
 */
public class CountFieldAggregatorFactory implements IFieldAggregateDescriptorFactory {

    private static final long serialVersionUID = 1L;

    private final boolean useObjectState;

    public CountFieldAggregatorFactory(boolean useObjectState) {
        this.useObjectState = useObjectState;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hyracks.dataflow.std.aggregations.
     * IFieldAggregateDescriptorFactory
     * #createAggregator(org.apache.hyracks.api.context.IHyracksTaskContext,
     * org.apache.hyracks.api.dataflow.value.RecordDescriptor,
     * org.apache.hyracks.api.dataflow.value.RecordDescriptor)
     */
    @Override
    public IFieldAggregateDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor) throws HyracksDataException {
        return new IFieldAggregateDescriptor() {

            @Override
            public void reset() {
            }

            @Override
            public void outputPartialResult(DataOutput fieldOutput, byte[] data, int offset, AggregateState state)
                    throws HyracksDataException {
                int count;
                if (!useObjectState) {
                    count = IntegerPointable.getInteger(data, offset);
                } else {
                    count = (Integer) state.state;
                }
                try {
                    fieldOutput.writeInt(count);
                } catch (IOException e) {
                    throw new HyracksDataException("I/O exception when writing aggregation to the output buffer.");
                }
            }

            @Override
            public void outputFinalResult(DataOutput fieldOutput, byte[] data, int offset, AggregateState state)
                    throws HyracksDataException {
                int count;
                if (!useObjectState) {
                    count = IntegerPointable.getInteger(data, offset);
                } else {
                    count = (Integer) state.state;
                }
                try {
                    fieldOutput.writeInt(count);
                } catch (IOException e) {
                    throw new HyracksDataException("I/O exception when writing aggregation to the output buffer.");
                }
            }

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex, DataOutput fieldOutput, AggregateState state)
                    throws HyracksDataException {
                int count = 1;
                if (!useObjectState) {
                    try {
                        fieldOutput.writeInt(count);
                    } catch (IOException e) {
                        throw new HyracksDataException("I/O exception when initializing the aggregator.");
                    }
                } else {
                    state.state = count;
                }
            }

            public boolean needsObjectState() {
                return useObjectState;
            }

            public boolean needsBinaryState() {
                return !useObjectState;
            }

            public AggregateState createState() {
                return new AggregateState(new Integer(0));
            }

            @Override
            public void close() {

            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset,
                    AggregateState state) throws HyracksDataException {
                int count = 1;
                if (!useObjectState) {
                    ByteBuffer buf = ByteBuffer.wrap(data);
                    count += buf.getInt(offset);
                    buf.putInt(offset, count);
                } else {
                    count += (Integer) state.state;
                    state.state = count;
                }
            }
        };
    }

}
