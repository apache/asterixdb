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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.active.ActiveObjectId.ActiveObjectType;
import org.apache.asterix.common.active.ActiveRuntimeId;
import org.apache.asterix.common.active.ActiveRuntimeInputHandler;
import org.apache.asterix.common.active.api.IActiveManager;
import org.apache.asterix.common.active.api.IActiveRuntime.ActiveRuntimeType;
import org.apache.asterix.common.active.api.IActiveRuntime.Mode;
import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.channels.ChannelRuntime;
import org.apache.asterix.common.channels.ProcedureRuntimeId;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.metadata.active.ActiveMetaNodePushable;
import org.apache.asterix.metadata.feeds.FeedPolicyEnforcer;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * The runtime for @see{RepetitiveChannelOperationDescriptor}.
 */
public class RepetitiveChannelOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(ActiveMetaNodePushable.class.getName());

    /**
     * A policy enforcer that ensures dyanmic decisions for a feed are taken
     * in accordance with the associated ingestion policy
     **/
    private FeedPolicyEnforcer policyEnforcer;

    /**
     * The Active Runtime instance associated with the operator. Active Runtime
     * captures the state of the operator while the job is active.
     */
    private ChannelRuntime activeRuntime;

    /**
     * A unique identifier for the active job. For instance,
     * A feed connection instance represents
     * the flow of data from a feed to a dataset.
     **/
    protected ActiveJobId activeJobId;

    /**
     * Denotes the i'th operator instance in a setting where K operator
     * instances are scheduled to run in parallel
     **/
    private int partition;

    /** Total number of partitions available **/
    private int nPartitions;

    /** Type associated with the core feed operator **/
    protected ActiveRuntimeType runtimeType;

    /** The (singleton) instance of IFeedManager **/
    protected IActiveManager activeManager;

    private FrameTupleAccessor fta;

    private final IHyracksTaskContext ctx;

    private final String operandId;

    /** The pre-processor associated with this runtime **/
    private ActiveRuntimeInputHandler inputSideHandler;

    private final ProcedureRuntimeId channelRuntimeId;
    private final long duration;
    private final String query;

    public RepetitiveChannelOperatorNodePushable(IHyracksTaskContext ctx, ActiveJobId channelJobId,
            FunctionSignature function, String duration, String subscriptionsName, String resultsName)
                    throws HyracksDataException {
        this.ctx = ctx;
        this.activeJobId = channelJobId;
        this.operandId = ActiveRuntimeId.DEFAULT_OPERAND_ID;
        this.runtimeType = ActiveRuntimeType.REPETITIVE;
        this.policyEnforcer = new FeedPolicyEnforcer(activeJobId, new HashMap<String, String>());
        this.channelRuntimeId = new ProcedureRuntimeId(channelJobId.getActiveId(), 0,
                ActiveRuntimeId.DEFAULT_OPERAND_ID);
        this.duration = findPeriod(duration);
        this.query = produceQuery(function, subscriptionsName, resultsName);
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.activeManager = runtimeCtx.getActiveManager();

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
        ProcedureRuntimeId runtimeId = new ProcedureRuntimeId(activeJobId.getDataverse(), activeJobId.getName(), 0,
                ActiveRuntimeId.DEFAULT_OPERAND_ID, ActiveObjectType.CHANNEL);
        try {
            activeRuntime = (ChannelRuntime) activeManager.getConnectionManager().getActiveRuntime(activeJobId,
                    runtimeId);
            if (activeRuntime == null) {
                initializeNewFeedRuntime(runtimeId);
            } else {
                reviveOldFeedRuntime(runtimeId);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
        activeRuntime.initialize(duration);
        writer.open();
    }

    public void drop() throws HyracksDataException {
        activeRuntime.drop();
        writer.close();
    }

    @Override
    public void open() throws HyracksDataException {

    }

    private void initializeNewFeedRuntime(ActiveRuntimeId runtimeId) throws Exception {
        this.fta = new FrameTupleAccessor(recordDesc);
        this.inputSideHandler = new ActiveRuntimeInputHandler(ctx, activeJobId, runtimeId, this,
                policyEnforcer.getFeedPolicyAccessor(), false, fta, recordDesc, activeManager, nPartitions);

        setupBasicRuntime(inputSideHandler);
    }

    private void reviveOldFeedRuntime(ActiveRuntimeId runtimeId) throws Exception {
        this.inputSideHandler = activeRuntime.getInputHandler();
        this.fta = new FrameTupleAccessor(recordDesc);
        this.setOutputFrameWriter(0, writer, recordDesc);
        activeRuntime.setMode(Mode.PROCESS);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Retreived state from the zombie instance " + runtimeType + " node.");
        }
    }

    private void setupBasicRuntime(ActiveRuntimeInputHandler inputHandler) throws Exception {
        this.setOutputFrameWriter(0, writer, recordDesc);
        ProcedureRuntimeId runtimeId = new ProcedureRuntimeId(activeJobId.getDataverse(), activeJobId.getName(), 0,
                ActiveRuntimeId.DEFAULT_OPERAND_ID, ActiveObjectType.CHANNEL);
        activeRuntime = new ChannelRuntime(runtimeId, inputHandler, writer, activeManager, activeJobId, query);
        activeManager.getConnectionManager().registerActiveRuntime(activeJobId, activeRuntime);
    }

    public ActiveJobId getActiveJobId() {
        return activeJobId;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        try {
            inputSideHandler.nextFrame(buffer);
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.info("Core Op:" + this.getDisplayName() + " fail ");
        }
        activeRuntime.setMode(Mode.FAIL);
        this.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

}
