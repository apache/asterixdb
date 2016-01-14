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
package org.apache.asterix.metadata.active;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.active.ActiveRuntime;
import org.apache.asterix.common.active.ActiveRuntimeId;
import org.apache.asterix.common.active.ActiveRuntimeInputHandler;
import org.apache.asterix.common.active.api.IActiveManager;
import org.apache.asterix.common.active.api.IActiveRuntime.ActiveRuntimeType;
import org.apache.asterix.common.active.api.IActiveRuntime.Mode;
import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.external.feeds.FeedPolicyEnforcer;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class ActiveMetaNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(ActiveMetaNodePushable.class.getName());

    /** Runtime node pushable corresponding to the core feed operator **/
    protected AbstractUnaryInputUnaryOutputOperatorNodePushable coreOperator;

    /**
     * A policy enforcer that ensures dyanmic decisions for a feed are taken
     * in accordance with the associated ingestion policy
     **/
    private FeedPolicyEnforcer policyEnforcer;

    /**
     * The Active Runtime instance associated with the operator. Active Runtime
     * captures the state of the operator while the job is active.
     */
    protected ActiveRuntime activeRuntime;

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
    protected int partition;

    /** Total number of partitions available **/
    private int nPartitions;

    /** Type associated with the core feed operator **/
    protected ActiveRuntimeType runtimeType;

    /** The (singleton) instance of IFeedManager **/
    protected IActiveManager activeManager;

    private FrameTupleAccessor fta;

    private final IHyracksTaskContext ctx;

    protected final String operandId;

    /** The pre-processor associated with this runtime **/
    protected ActiveRuntimeInputHandler inputSideHandler;

    public ActiveMetaNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider, int partition,
            int nPartitions, IOperatorDescriptor coreOperator, ActiveJobId activeJobId,
            Map<String, String> feedPolicyProperties, String operationId) throws HyracksDataException {
        this.ctx = ctx;
        this.coreOperator = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
        this.policyEnforcer = new FeedPolicyEnforcer(activeJobId, feedPolicyProperties);
        this.partition = partition;
        this.nPartitions = nPartitions;
        this.activeJobId = activeJobId;
        this.runtimeType = ActiveRuntimeType.OTHER;
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.activeManager = runtimeCtx.getActiveManager();
        this.operandId = operationId;
    }

    @Override
    public void open() throws HyracksDataException {
        ActiveRuntimeId runtimeId = new ActiveRuntimeId(runtimeType, partition, operandId);
        try {
            activeRuntime = activeManager.getConnectionManager().getActiveRuntime(activeJobId, runtimeId);
            if (activeRuntime == null) {
                initializeNewFeedRuntime(runtimeId);
            } else {
                reviveOldFeedRuntime(runtimeId);
            }
            coreOperator.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    protected void initializeNewFeedRuntime(ActiveRuntimeId runtimeId) throws Exception {
        this.fta = new FrameTupleAccessor(recordDesc);
        this.inputSideHandler = new ActiveRuntimeInputHandler(ctx, activeJobId, runtimeId, coreOperator,
                policyEnforcer.getFeedPolicyAccessor(), false, fta, recordDesc, activeManager, nPartitions);

        setupBasicRuntime(inputSideHandler);
    }

    protected void reviveOldFeedRuntime(ActiveRuntimeId runtimeId) throws Exception {
        this.inputSideHandler = activeRuntime.getInputHandler();
        this.fta = new FrameTupleAccessor(recordDesc);
        coreOperator.setOutputFrameWriter(0, writer, recordDesc);
        activeRuntime.setMode(Mode.PROCESS);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Retreived state from the zombie instance " + runtimeType + " node.");
        }
    }

    protected void setupBasicRuntime(ActiveRuntimeInputHandler inputHandler) throws Exception {
        coreOperator.setOutputFrameWriter(0, writer, recordDesc);
        ActiveRuntimeId runtimeId = new ActiveRuntimeId(runtimeType, partition, operandId);
        activeRuntime = new ActiveRuntime(runtimeId, inputHandler, writer);
        //TODO: registering as type "other" which is a problem since ALL of the runtimes in this active pipeline will be other (indistinguishable and might be overwridden)
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
            LOGGER.info("Core Op:" + coreOperator.getDisplayName() + " fail ");
        }
        activeRuntime.setMode(Mode.FAIL);
        coreOperator.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            coreOperator.close();
        } catch (Exception e) {
            e.printStackTrace();
            // ignore
        } finally {
            if (inputSideHandler != null) {
                inputSideHandler.close();
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Ending Operator  " + this.activeRuntime.getRuntimeId());
            }
        }
    }

}