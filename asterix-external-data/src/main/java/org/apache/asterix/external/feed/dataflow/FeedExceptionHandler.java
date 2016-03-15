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
package org.apache.asterix.external.feed.dataflow;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.FrameDataException;
import org.apache.asterix.external.feed.api.IExceptionHandler;
import org.apache.asterix.external.feed.api.IFeedManager;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.util.FeedFrameUtil;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class FeedExceptionHandler implements IExceptionHandler {

    private static Logger LOGGER = Logger.getLogger(FeedExceptionHandler.class.getName());

    //TODO: Enable logging
    private final IHyracksTaskContext ctx;
    private final FrameTupleAccessor fta;

    public FeedExceptionHandler(IHyracksTaskContext ctx, FrameTupleAccessor fta, RecordDescriptor recordDesc,
            IFeedManager feedManager, FeedConnectionId connectionId) {
        this.ctx = ctx;
        this.fta = fta;
    }

    public void prettyPrint(ByteBuffer frame) {
        fta.reset(frame);
        fta.prettyPrint();
    }

    @Override
    public ByteBuffer handleException(Exception e, ByteBuffer frame) {
        try {
            if (e instanceof FrameDataException) {
                fta.reset(frame);
                FrameDataException fde = (FrameDataException) e;
                int tupleIndex = fde.getTupleIndex();
                try {
                    logExceptionCausingTuple(tupleIndex, e);
                } catch (Exception ex) {
                    ex.addSuppressed(e);
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Unable to log exception causing tuple due to..." + ex.getMessage());
                    }
                }
                return FeedFrameUtil.removeBadTuple(ctx, tupleIndex, fta);
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
    }
}
