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
package org.apache.asterix.metadata.channels;

import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.channels.ChannelRuntimeId;
import org.apache.asterix.common.feeds.api.ActiveRuntimeId;
import org.apache.asterix.common.feeds.api.IActiveRuntime.ActiveRuntimeType;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.metadata.feeds.FeedIntakeOperatorNodePushable;
import org.apache.asterix.metadata.feeds.FeedMetaNodePushable;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.json.JSONException;

/**
 * The runtime for @see{RepetitiveChannelOperationDescriptor}.
 */
public class RepetitiveChannelOperatorNodePushable extends FeedMetaNodePushable {

    private static Logger LOGGER = Logger.getLogger(FeedIntakeOperatorNodePushable.class.getName());

    private final ChannelRuntimeId channelRuntimeId;
    private final long duration;
    private final String query;
    private boolean complete = false;
    private Timer timer;

    public RepetitiveChannelOperatorNodePushable(IHyracksTaskContext ctx, ActiveJobId channelJobId,
            FunctionSignature function, String duration, String subscriptionsName, String resultsName)
            throws HyracksDataException {
        super(ctx, null, 0, 1, null, channelJobId, new HashMap<String, String>(), ActiveRuntimeId.DEFAULT_OPERAND_ID);
        this.runtimeType = ActiveRuntimeType.REPETITIVE;
        this.channelRuntimeId = new ChannelRuntimeId(channelJobId.getActiveId());
        this.duration = findPeriod(duration);
        this.query = produceQuery(function, subscriptionsName, resultsName);
        timer = new Timer();

    }

    private long findPeriod(String duration) {
        //TODO: Allow Repetitive Channels to use YMD durations
        String hoursMinutesSeconds = "";
        if (duration.indexOf("T") != -1) {
            hoursMinutesSeconds = duration.substring(duration.indexOf("T") + 1);
        }
        double seconds = 0;
        if (hoursMinutesSeconds != "") {
            int pos = 0;
            if (hoursMinutesSeconds.indexOf("H") != -1) {
                Double hours = Double.parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf("H")));
                seconds += (hours * 60 * 60);
                pos = hoursMinutesSeconds.indexOf("H") + 1;

            }
            if (hoursMinutesSeconds.indexOf("M") != -1) {
                Double minutes = Double
                        .parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf("M")));
                seconds += (minutes * 60);
                pos = hoursMinutesSeconds.indexOf("M") + 1;
            }
            if (hoursMinutesSeconds.indexOf("S") != -1) {
                Double s = Double.parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf("S")));
                seconds += (s);
            }

        }
        return (long) (seconds * 1000);
    }

    private String produceQuery(FunctionSignature function, String subscriptionsName, String resultsName) {
        //insert into resultsTableName(
        //for $sub in dataset subscriptionsTableName
        //for $result in Function(parameters...)
        //return {
        //    "subscription-id":$sub.subscription-id,
        //    "execution-time":current-datetime(),
        //    "result":$result
        //}
        //);
        StringBuilder builder = new StringBuilder();
        builder.append("use dataverse " + activeJobId.getDataverse() + ";" + "\n");
        builder.append("insert into dataset " + resultsName + " ");
        builder.append(" (" + " for $sub in dataset " + subscriptionsName + "\n");
        builder.append(" for $result in " + function.getName() + "(");

        int i = 0;
        for (; i < function.getArity() - 1; i++) {
            builder.append("$sub.param" + i + ",");
        }
        builder.append("$sub.param" + i + ")\n");
        builder.append("return {\n");
        builder.append("\"subscription-id\":$sub.subscription-id,");
        builder.append("\"execution-time\":current-datetime(),");
        builder.append("\"result\":$result");

        builder.append("}");
        builder.append(")");
        builder.append(";");
        return builder.toString();
    }

    @Override
    public void initialize() throws HyracksDataException {
        //activeManager.getConnectionManager().registerActiveRuntime(channelJobId, this);
        timer.schedule(new AQLTask(), 0, duration);
        writer.open();
        while (!complete) {

        }
    }

    public void drop() throws HyracksDataException {
        timer.cancel();
        writer.close();
        complete = true;
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

}
