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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.common.feeds.FeedConstants.StatisticsConstants;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;

/**
 * Allows caching of feed frames. This class is used in providing upstream backup.
 * The tuples at the intake layer are held in this cache until these are acked by
 * the storage layer post their persistence. On receiving an ack, appropriate tuples
 * (recordsId < ackedRecordId) are dropped from the cache.
 */
public class FeedFrameCache extends MessageReceiver<ByteBuffer> {

    /**
     * Value represents a cache feed frame
     * Key represents the largest record Id in the frame.
     * At the intake side, the largest record id corresponds to the last record in the frame
     **/
    private final Map<Integer, ByteBuffer> orderedCache;
    private final FrameTupleAccessor tupleAccessor;
    private final IFrameWriter frameWriter;
    private final IHyracksTaskContext ctx;

    public FeedFrameCache(IHyracksTaskContext ctx, FrameTupleAccessor tupleAccessor, IFrameWriter frameWriter) {
        this.tupleAccessor = tupleAccessor;
        this.frameWriter = frameWriter;
        /** A LinkedHashMap ensures entries are retrieved in order of their insertion **/
        this.orderedCache = new LinkedHashMap<Integer, ByteBuffer>();
        this.ctx = ctx;
    }

    @Override
    public void processMessage(ByteBuffer frame) throws Exception {
        int lastRecordId = getLastRecordId(frame);
        ByteBuffer clone = cloneFrame(frame);
        orderedCache.put(lastRecordId, clone);
    }

    public void dropTillRecordId(int recordId) {
        List<Integer> dropRecordIds = new ArrayList<Integer>();
        for (Entry<Integer, ByteBuffer> entry : orderedCache.entrySet()) {
            int recId = entry.getKey();
            if (recId <= recordId) {
                dropRecordIds.add(recId);
            } else {
                break;
            }
        }
        for (Integer r : dropRecordIds) {
            orderedCache.remove(r);
        }
    }

    public void replayRecords(int startingRecordId) throws HyracksDataException {
        boolean replayPositionReached = false;
        for (Entry<Integer, ByteBuffer> entry : orderedCache.entrySet()) {
            // the key increases monotonically
            int maxRecordIdInFrame = entry.getKey();
            if (!replayPositionReached) {
                if (startingRecordId < maxRecordIdInFrame) {
                    replayFrame(startingRecordId, entry.getValue());
                    break;
                } else {
                    continue;
                }
            }
        }
    }

    /**
     * Replay the frame from the tuple (inclusive) with recordId as specified.
     * 
     * @param recordId
     * @param frame
     * @throws HyracksDataException
     */
    private void replayFrame(int recordId, ByteBuffer frame) throws HyracksDataException {
        tupleAccessor.reset(frame);
        int nTuples = tupleAccessor.getTupleCount();
        for (int i = 0; i < nTuples; i++) {
            int rid = getRecordIdAtTupleIndex(i, frame);
            if (rid == recordId) {
                ByteBuffer slicedFrame = splitFrame(i, frame);
                replayFrame(slicedFrame);
                break;
            }
        }
    }

    private ByteBuffer splitFrame(int beginTupleIndex, ByteBuffer frame) throws HyracksDataException {
        IFrame slicedFrame = new VSizeFrame(ctx);
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(slicedFrame, true);
        int totalTuples = tupleAccessor.getTupleCount();
        for (int ti = beginTupleIndex; ti < totalTuples; ti++) {
            appender.append(tupleAccessor, ti);
        }
        return slicedFrame.getBuffer();
    }

    /**
     * Replay the frame
     * 
     * @param frame
     * @throws HyracksDataException
     */
    private void replayFrame(ByteBuffer frame) throws HyracksDataException {
        frameWriter.nextFrame(frame);
    }

    private int getLastRecordId(ByteBuffer frame) {
        tupleAccessor.reset(frame);
        int nTuples = tupleAccessor.getTupleCount();
        return getRecordIdAtTupleIndex(nTuples - 1, frame);
    }

    private int getRecordIdAtTupleIndex(int tupleIndex, ByteBuffer frame) {
        tupleAccessor.reset(frame);
        int recordStart = tupleAccessor.getTupleStartOffset(tupleIndex) + tupleAccessor.getFieldSlotsLength();
        int openPartOffset = frame.getInt(recordStart + 6);
        int numOpenFields = frame.getInt(recordStart + openPartOffset);
        int recordIdOffset = frame.getInt(recordStart + openPartOffset + 4 + numOpenFields * 8
                + StatisticsConstants.INTAKE_TUPLEID.length() + 2 + 1);
        int lastRecordId = frame.getInt(recordStart + recordIdOffset);
        return lastRecordId;
    }

    private ByteBuffer cloneFrame(ByteBuffer frame) {
        ByteBuffer clone = ByteBuffer.allocate(frame.capacity());
        System.arraycopy(frame.array(), 0, clone.array(), 0, frame.limit());
        return clone;
    }

    public void replayAll() throws HyracksDataException {
        for (Entry<Integer, ByteBuffer> entry : orderedCache.entrySet()) {
            ByteBuffer frame = entry.getValue();
            frameWriter.nextFrame(frame);
        }
    }
}
