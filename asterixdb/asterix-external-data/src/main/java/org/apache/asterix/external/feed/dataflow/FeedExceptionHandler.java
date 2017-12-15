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

import org.apache.asterix.common.exceptions.IExceptionHandler;
import org.apache.asterix.external.util.FeedFrameUtil;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FeedExceptionHandler implements IExceptionHandler {

    private static final Logger LOGGER = LogManager.getLogger();

    // TODO: Enable logging
    private final IHyracksTaskContext ctx;
    private final FrameTupleAccessor fta;

    public FeedExceptionHandler(IHyracksTaskContext ctx, FrameTupleAccessor fta) {
        this.ctx = ctx;
        this.fta = fta;
    }

    @Override
    public ByteBuffer handle(HyracksDataException th, ByteBuffer frame) {
        try {
            if (th.getErrorCode() == ErrorCode.ERROR_PROCESSING_TUPLE) {
                // TODO(amoudi): add check for cause. cause should be either cast or duplicate key
                fta.reset(frame);
                int tupleIndex = (int) (th.getParams()[0]);
                try {
                    logExceptionCausingTuple(tupleIndex, th);
                } catch (Exception ex) {
                    ex.addSuppressed(th);
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Unable to log exception causing tuple due to..." + ex.getMessage());
                    }
                }
                // TODO: Improve removeBadTuple. Right now, it creates lots of objects
                return FeedFrameUtil.removeBadTuple(ctx, tupleIndex, fta);
            } else {
                return null;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Unable to handle exception " + exception.getMessage());
            }
            return null;
        }
    }

    // TODO: Fix logging of exceptions
    private void logExceptionCausingTuple(int tupleIndex, Throwable e) throws HyracksDataException {
        LOGGER.log(Level.WARN, e.getMessage(), e);
    }
}
