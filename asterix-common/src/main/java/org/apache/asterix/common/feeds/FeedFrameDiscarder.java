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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.context.IHyracksTaskContext;

public class FeedFrameDiscarder {

    private static final Logger LOGGER = Logger.getLogger(FeedFrameSpiller.class.getName());

    private final IHyracksTaskContext ctx;
    private final FeedRuntimeInputHandler inputHandler;
    private final FeedConnectionId connectionId;
    private final FeedRuntimeId runtimeId;
    private final FeedPolicyAccessor policyAccessor;
    private final float maxFractionDiscard;
    private int nDiscarded;

    public FeedFrameDiscarder(IHyracksTaskContext ctx, FeedConnectionId connectionId, FeedRuntimeId runtimeId, 
            FeedPolicyAccessor policyAccessor, FeedRuntimeInputHandler inputHandler) throws IOException {
        this.ctx = ctx;
        this.connectionId = connectionId;
        this.runtimeId = runtimeId;
        this.policyAccessor = policyAccessor;
        this.inputHandler = inputHandler;
        this.maxFractionDiscard = policyAccessor.getMaxFractionDiscard();
    }

    public boolean processMessage(ByteBuffer message) {
        if (policyAccessor.getMaxFractionDiscard() != 0) {
            long nProcessed = inputHandler.getProcessed();
            long discardLimit = (long) (nProcessed * maxFractionDiscard);
            if (nDiscarded >= discardLimit) {
                return false;
            }
            nDiscarded++;
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Discarded frame by " + connectionId + " (" + runtimeId + ")" + " count so far  ("
                        + nDiscarded + ") Limit [" + discardLimit + "]");
            }
            return true;
        }
        return false;
    }

}
