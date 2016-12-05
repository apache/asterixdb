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
package org.apache.asterix.active;

import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public abstract class ActiveSourceOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable
        implements IActiveRuntime {

    protected final IHyracksTaskContext ctx;
    protected final ActiveManager activeManager;
    /** A unique identifier for the runtime **/
    protected final ActiveRuntimeId runtimeId;
    private volatile boolean done = false;

    public ActiveSourceOperatorNodePushable(IHyracksTaskContext ctx, ActiveRuntimeId runtimeId) {
        this.ctx = ctx;
        activeManager = (ActiveManager) ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject()).getActiveManager();
        this.runtimeId = runtimeId;
    }

    @Override
    public ActiveRuntimeId getRuntimeId() {
        return runtimeId;
    }

    /**
     * Starts the active job. This method must not return until the job has finished
     *
     * @throws HyracksDataException
     * @throws InterruptedException
     */
    protected abstract void start() throws HyracksDataException, InterruptedException;

    @Override
    public final void stop() throws HyracksDataException, InterruptedException {
        abort();
        synchronized (this) {
            while (!done) {
                wait();
            }
        }
    }

    /**
     * called from a different thread. This method stops the active node and force the start() call to return
     *
     * @throws HyracksDataException
     * @throws InterruptedException
     */
    protected abstract void abort() throws HyracksDataException, InterruptedException;

    @Override
    public String toString() {
        return runtimeId.toString();
    }

    @Override
    public final void initialize() throws HyracksDataException {
        activeManager.registerRuntime(this);
        try {
            // notify cc that runtime has been registered
            ctx.sendApplicationMessageToCC(new ActivePartitionMessage(runtimeId, ctx.getJobletContext().getJobId(),
                    ActivePartitionMessage.ACTIVE_RUNTIME_REGISTERED), null);
            start();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HyracksDataException(e);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            synchronized (this) {
                done = true;
                notifyAll();
            }
        }
    }

    @Override
    public final void deinitialize() throws HyracksDataException {
        activeManager.deregisterRuntime(runtimeId);
        try {
            ctx.sendApplicationMessageToCC(new ActivePartitionMessage(runtimeId, ctx.getJobletContext().getJobId(),
                    ActivePartitionMessage.ACTIVE_RUNTIME_DEREGISTERED), null);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }


    @Override
    public final IFrameWriter getInputFrameWriter(int index) {
        return null;
    }
}
