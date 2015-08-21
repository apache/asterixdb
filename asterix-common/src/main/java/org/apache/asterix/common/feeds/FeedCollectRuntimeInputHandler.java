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
package edu.uci.ics.asterix.common.feeds;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

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
