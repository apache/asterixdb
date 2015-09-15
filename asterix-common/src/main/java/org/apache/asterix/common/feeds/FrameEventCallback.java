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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.api.IFeedRuntime.Mode;
import org.apache.asterix.common.feeds.api.IFrameEventCallback;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FrameEventCallback implements IFrameEventCallback {

    private static final Logger LOGGER = Logger.getLogger(FrameEventCallback.class.getName());

    private final FeedPolicyAccessor fpa;
    private final FeedRuntimeInputHandler inputSideHandler;
    private IFrameWriter coreOperator;

    public FrameEventCallback(FeedPolicyAccessor fpa, FeedRuntimeInputHandler inputSideHandler,
            IFrameWriter coreOperator) {
        this.fpa = fpa;
        this.inputSideHandler = inputSideHandler;
        this.coreOperator = coreOperator;
    }

    @Override
    public void frameEvent(FrameEvent event) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Frame Event for " + inputSideHandler.getRuntimeId() + " " + event);
        }
        if (!event.equals(FrameEvent.FINISHED_PROCESSING_SPILLAGE)
                && inputSideHandler.getMode().equals(Mode.PROCESS_SPILL)) {
            return;
        }
        switch (event) {
            case PENDING_WORK_THRESHOLD_REACHED:
                if (fpa.spillToDiskOnCongestion()) {
                    inputSideHandler.setMode(Mode.SPILL);
                } else if (fpa.discardOnCongestion()) {
                    inputSideHandler.setMode(Mode.DISCARD);
                } else if (fpa.throttlingEnabled()) {
                    inputSideHandler.setThrottlingEnabled(true);
                } else {
                    try {
                        inputSideHandler.reportUnresolvableCongestion();
                    } catch (HyracksDataException e) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Unable to report congestion!!!");
                        }
                    }
                }
                break;
            case FINISHED_PROCESSING:
                inputSideHandler.setFinished(true);
                synchronized (coreOperator) {
                    coreOperator.notifyAll();
                }
                break;
            case PENDING_WORK_DONE:
                switch (inputSideHandler.getMode()) {
                    case SPILL:
                    case DISCARD:
                    case POST_SPILL_DISCARD:
                        inputSideHandler.setMode(Mode.PROCESS);
                        break;
                    default:
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Received " + event + " ignoring as operating in " + inputSideHandler.getMode());
                        }
                }
                break;
            case FINISHED_PROCESSING_SPILLAGE:
                inputSideHandler.setMode(Mode.PROCESS);
                break;
            default:
                break;
        }
    }

    public void setCoreOperator(IFrameWriter coreOperator) {
        this.coreOperator = coreOperator;
    }

}