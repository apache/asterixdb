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

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.active.ActiveRuntimeId;
import org.apache.asterix.common.active.ActiveRuntimeInputHandler;
import org.apache.asterix.common.active.api.IActiveRuntime.ActiveRuntimeType;
import org.apache.asterix.common.channels.ProcedureRuntime;
import org.apache.asterix.common.channels.ProcedureRuntimeId;
import org.apache.asterix.metadata.active.ActiveMetaNodePushable;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ProcedureMetaNodePushable extends ActiveMetaNodePushable {

    private static final Logger LOGGER = Logger.getLogger(ProcedureMetaNodePushable.class.getName());

    public ProcedureMetaNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
            int partition, int nPartitions, IOperatorDescriptor coreOperator, ActiveJobId activeJobId,
            Map<String, String> feedPolicyProperties, String operationId) throws HyracksDataException {
        super(ctx, recordDescProvider, partition, nPartitions, coreOperator, activeJobId, feedPolicyProperties,
                operationId);
        this.runtimeType = ActiveRuntimeType.PROCEDURE;
    }

    public void runProcedure() throws HyracksDataException {
        //What's going to happen to the result? We need to somehow get the context of the procedural call here
        coreOperator.open();
    }

    public void endProcedure() throws HyracksDataException {
        coreOperator.close();
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

        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }

        boolean complete = ((ProcedureRuntime) activeRuntime).waitForCompletion();

        writer.open();
        coreOperator.open();

        //activeManager.getConnectionManager().deRegisterActiveRuntime(activeJobId, activeRuntime.getRuntimeId());
        //  writer.close();
        //  inputSideHandler.close();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Ending Procedure Operator:" + activeRuntime.getRuntimeId());
        }

    }

    @Override
    protected void setupBasicRuntime(ActiveRuntimeInputHandler inputHandler) throws Exception {
        coreOperator.setOutputFrameWriter(0, writer, recordDesc);
        ActiveRuntimeId runtimeId = new ProcedureRuntimeId(activeJobId.getActiveId(), partition, operandId);
        activeRuntime = new ProcedureRuntime(runtimeId, inputHandler, writer);
        //TODO: registered as type procedure (only the ets is currently registered this way)
        activeManager.getConnectionManager().registerActiveRuntime(activeJobId, activeRuntime);
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

    @Override
    public String toString() {
        return "ActiveMetaProcedureHead [" + coreOperator + " ]";
    }

}