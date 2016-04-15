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

package org.apache.hyracks.dataflow.std.parallel.histogram;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * @author michael
 */
public class SampleReaderOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private final IHyracksTaskContext ctx;
    private final int[] sampleFields;
    private final int sampleBasis;
    private final int frameLimit;
    private final int outputLimit;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private RecordDescriptor outDesc;
    private RecordDescriptor inDesc;

    /**
     * 
     */
    public SampleReaderOperatorNodePushable(final IHyracksTaskContext ctx, int[] sampleFields, int sampleBasis,
            int frameLimit, IRecordDescriptorProvider recordDescProvider, int outputLimit, RecordDescriptor inDesc,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            final int partition, final int nPartitions) {
        this.ctx = ctx;
        this.sampleFields = sampleFields;
        this.sampleBasis = sampleBasis;
        this.frameLimit = frameLimit;
        this.outputLimit = outputLimit;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        this.inDesc = inDesc;
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] outSchema = new ISerializerDeserializer[sampleFields.length];
        for (int i = 0; i < sampleFields.length; i++) {
            outSchema[i] = inDesc.getFields()[sampleFields[i]];
        }
        this.outDesc = new RecordDescriptor(outSchema);
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.api.comm.IFrameWriter#open()
     */
    @Override
    public void open() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.api.comm.IFrameWriter#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.api.comm.IFrameWriter#fail()
     */
    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.api.comm.IFrameWriter#close()
     */
    @Override
    public void close() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

}
