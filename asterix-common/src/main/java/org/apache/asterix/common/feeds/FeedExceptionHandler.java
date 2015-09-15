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

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.FrameDataException;
import org.apache.asterix.common.feeds.api.IExceptionHandler;
import org.apache.asterix.common.feeds.api.IFeedManager;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class FeedExceptionHandler implements IExceptionHandler {

    private static final Logger LOGGER = Logger.getLogger(FeedExceptionHandler.class.getName());

    private final IHyracksTaskContext ctx;
    private final FrameTupleAccessor fta;
    private final RecordDescriptor recordDesc;
    private final IFeedManager feedManager;
    private final FeedConnectionId connectionId;

    public FeedExceptionHandler(IHyracksTaskContext ctx, FrameTupleAccessor fta, RecordDescriptor recordDesc,
            IFeedManager feedManager, FeedConnectionId connectionId) {
        this.ctx = ctx;
        this.fta = fta;
        this.recordDesc = recordDesc;
        this.feedManager = feedManager;
        this.connectionId = connectionId;
    }

    public ByteBuffer handleException(Exception e, ByteBuffer frame) {
        try {
            if (e instanceof FrameDataException) {
                fta.reset(frame);
                FrameDataException fde = (FrameDataException) e;
                int tupleIndex = fde.getTupleIndex();

                // logging 
                try {
                    logExceptionCausingTuple(tupleIndex, e);
                } catch (Exception ex) {
                    ex.addSuppressed(e);
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Unable to log exception causing tuple due to..." + ex.getMessage());
                    }
                }
                // slicing
                return FeedFrameUtil.getSlicedFrame(ctx, tupleIndex, fta);
            } else {
                return null;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to handle exception " + exception.getMessage());
            }
            return null;
        }
    }

    private void logExceptionCausingTuple(int tupleIndex, Exception e) throws HyracksDataException, AsterixException {

        ByteBufferInputStream bbis = new ByteBufferInputStream();
        DataInputStream di = new DataInputStream(bbis);

        int start = fta.getTupleStartOffset(tupleIndex) + fta.getFieldSlotsLength();
        bbis.setByteBuffer(fta.getBuffer(), start);

        Object[] record = new Object[recordDesc.getFieldCount()];

        for (int i = 0; i < record.length; ++i) {
            Object instance = recordDesc.getFields()[i].deserialize(di);
            if (i == 0) {
                String tuple = String.valueOf(instance);
                feedManager.getFeedMetadataManager().logTuple(connectionId, tuple, e.getMessage(), feedManager);
            } else {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning(", " + String.valueOf(instance));
                }
            }
        }

    }
}
