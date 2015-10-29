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
package org.apache.asterix.common.channels;

import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.feeds.ActiveRuntime;
import org.apache.asterix.common.feeds.ActiveRuntimeInputHandler;
import org.apache.asterix.common.feeds.api.ActiveRuntimeId;
import org.apache.asterix.common.feeds.api.IActiveManager;
import org.apache.asterix.common.feeds.message.RepetitiveChannelXAQLMessage;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.json.JSONException;

public class ChannelRuntime extends ActiveRuntime {

    private static final Logger LOGGER = Logger.getLogger(ChannelRuntime.class.getName());

    private Timer timer;
    private boolean complete = false;
    protected IActiveManager activeManager;
    protected ActiveJobId activeJobId;
    private final String query;

    public ChannelRuntime(ActiveRuntimeId runtimeId, ActiveRuntimeInputHandler inputHandler, IFrameWriter frameWriter,
            IActiveManager activeManager, ActiveJobId activeJobId, String query) {
        super(runtimeId, inputHandler, frameWriter);
        this.activeJobId = activeJobId;
        this.query = query;
        this.activeManager = activeManager;
        timer = new Timer();
    }

    private class AQLTask extends TimerTask {
        public void run() {
            LOGGER.info("Executing Channel: " + activeJobId.toString());
            RepetitiveChannelXAQLMessage xAqlMessage = new RepetitiveChannelXAQLMessage(activeJobId, query);
            activeManager.getFeedMessageService().sendMessage(xAqlMessage);
            if (LOGGER.isLoggable(Level.INFO)) {
                try {
                    LOGGER.info(" Sent " + xAqlMessage.toJSON());
                } catch (JSONException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    public void initialize(long duration) {
        timer.schedule(new AQLTask(), 0, duration);
        while (!complete) {

        }

    }

    //TODO: I don't think that this actually gets rid of the operator. Not sure
    public void drop() throws HyracksDataException {
        timer.cancel();
        complete = true;
    }
}
