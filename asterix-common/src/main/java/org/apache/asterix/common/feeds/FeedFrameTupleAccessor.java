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
package org.apache.asterix.common.feeds;

import java.nio.ByteBuffer;

import org.apache.asterix.common.feeds.FeedConstants.StatisticsConstants;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class FeedFrameTupleAccessor implements IFrameTupleAccessor {

    private final FrameTupleAccessor frameAccessor;
    private final int numOpenFields;

    public FeedFrameTupleAccessor(FrameTupleAccessor frameAccessor) {
        this.frameAccessor = frameAccessor;
        int firstRecordStart = frameAccessor.getTupleStartOffset(0) + frameAccessor.getFieldSlotsLength();
        int openPartOffsetOrig = frameAccessor.getBuffer().getInt(firstRecordStart + 6);
        numOpenFields = frameAccessor.getBuffer().getInt(firstRecordStart + openPartOffsetOrig);
    }

    public int getFeedIntakePartition(int tupleIndex) {
        ByteBuffer buffer = frameAccessor.getBuffer();
        int recordStart = frameAccessor.getTupleStartOffset(tupleIndex) + frameAccessor.getFieldSlotsLength();
        int openPartOffsetOrig = buffer.getInt(recordStart + 6);
        int partitionOffset = openPartOffsetOrig + 4 + 8 * numOpenFields
                + StatisticsConstants.INTAKE_PARTITION.length() + 2 + 1;
        return buffer.getInt(recordStart + partitionOffset);
    }
    
    

    @Override
    public int getFieldCount() {
        return frameAccessor.getFieldCount();
    }

    @Override
    public int getFieldSlotsLength() {
        return frameAccessor.getFieldSlotsLength();
    }

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return frameAccessor.getFieldEndOffset(tupleIndex, fIdx);
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return frameAccessor.getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return frameAccessor.getFieldLength(tupleIndex, fIdx);
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return frameAccessor.getTupleEndOffset(tupleIndex);
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        return frameAccessor.getTupleStartOffset(tupleIndex);
    }

    @Override
    public int getTupleCount() {
        return frameAccessor.getTupleCount();
    }

    @Override
    public ByteBuffer getBuffer() {
        return frameAccessor.getBuffer();
    }

    @Override
    public void reset(ByteBuffer buffer) {
        frameAccessor.reset(buffer);
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return frameAccessor.getAbsoluteFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        return frameAccessor.getTupleLength(tupleIndex);
    }

}
