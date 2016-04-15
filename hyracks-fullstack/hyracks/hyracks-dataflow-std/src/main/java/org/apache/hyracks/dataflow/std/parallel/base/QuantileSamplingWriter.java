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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.dataflow.std.parallel.IHistogram;
import org.apache.hyracks.dataflow.std.parallel.histogram.structures.AbstractStreamingHistogram;

/**
 * @author michael
 */
public class QuantileSamplingWriter extends AbstractSamplingWriter {
    private static int processed = 0;
    private final IHyracksTaskContext ctx;
    private final int[] sampleFields;
    private final int sampleBasis;
    private final IBinaryComparator[] comparators;
    private final RecordDescriptor inDesc;
    private final RecordDescriptor outDesc;
    private final boolean outputPartial;

    private static final int DEFAULT_ELASTIC = 10;
    private static final double DEFAULT_MU = 1.7;

    private final IHistogram merging;
    private final IHistogram current;

    public QuantileSamplingWriter(IHyracksTaskContext ctx, int[] sampleFields, int sampleBasis,
            IBinaryComparator[] comparators, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc,
            IFrameWriter writer, boolean outputPartial) throws HyracksDataException {
        super(ctx, sampleFields, sampleBasis, comparators, inRecordDesc, outRecordDesc, writer);
        this.ctx = ctx;
        this.sampleFields = sampleFields;
        this.sampleBasis = sampleBasis;
        this.comparators = comparators;
        this.inDesc = inRecordDesc;
        this.outDesc = outRecordDesc;
        this.outputPartial = outputPartial;
        this.merging = new AbstractStreamingHistogram(comparators, sampleBasis, DEFAULT_ELASTIC, DEFAULT_MU);
        this.current = new AbstractStreamingHistogram(comparators, sampleBasis, DEFAULT_ELASTIC, DEFAULT_MU);
    }

    public QuantileSamplingWriter(IHyracksTaskContext ctx, int[] sampleFields, int sampleBasis,
            IBinaryComparator[] comparators, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc,
            IFrameWriter writer) throws HyracksDataException {
        this(ctx, sampleFields, sampleBasis, comparators, inRecordDesc, outRecordDesc, writer, false);
    }

    @Override
    public void open() throws HyracksDataException {
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
    }

    @Override
    public void fail() throws HyracksDataException {
        isFailed = true;
        appenderWrapper.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        if (!isFailed && !isFirst) {
            appenderWrapper.flush();
        }
        aggregator.close();
        aggregateState.close();
        appenderWrapper.close();
    }
}
