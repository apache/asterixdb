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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.common.feeds.api.IFeedManager;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class FeedCollectRuntimeInputHandler extends FeedRuntimeInputHandler {

    private final FeedFrameCache feedFrameCache;

    public FeedCollectRuntimeInputHandler(IHyracksTaskContext ctx, FeedConnectionId connectionId, FeedRuntimeId runtimeId,
            IFrameWriter coreOperator, FeedPolicyAccessor fpa, boolean bufferingEnabled,
            FrameTupleAccessor fta, RecordDescriptor recordDesc, IFeedManager feedManager, int nPartitions)
            throws IOException {
        super(ctx, connectionId, runtimeId, coreOperator, fpa, bufferingEnabled, fta, recordDesc, feedManager,
                nPartitions);
        this.feedFrameCache = new FeedFrameCache(ctx, fta, coreOperator);
    }

    public void process(ByteBuffer frame) throws HyracksDataException {
        feedFrameCache.sendMessage(frame);
        super.process(frame);
    }

    public void replayFrom(int recordId) throws HyracksDataException {
        feedFrameCache.replayRecords(recordId);
    }

    public void dropTill(int recordId) {
        feedFrameCache.dropTillRecordId(recordId);
    }
    
    public void replayCached() throws HyracksDataException{
        feedFrameCache.replayAll();
    }

}
