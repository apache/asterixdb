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

package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.IoOperationCompleteListener;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.hyracks.util.trace.ITracer.Scope;
import org.apache.hyracks.util.trace.TraceUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class TracedIOOperation implements ILSMIOOperation {

    static final Logger LOGGER = LogManager.getLogger();

    protected final ILSMIOOperation ioOp;
    private final LSMIOOperationType ioOpType;
    private final ITracer tracer;
    private final long traceCategory;

    protected TracedIOOperation(ILSMIOOperation ioOp, ITracer tracer, long traceCategory) {
        this.ioOp = ioOp;
        this.tracer = tracer;
        this.ioOpType = ioOp.getIOOpertionType();
        this.traceCategory = traceCategory;
    }

    public static ILSMIOOperation wrap(final ILSMIOOperation ioOp, final ITracer tracer) {
        final String ioOpName = ioOp.getIOOpertionType().name().toLowerCase();
        final long traceCategory = tracer.getRegistry().get(TraceUtils.INDEX_IO_OPERATIONS);
        if (tracer.isEnabled(traceCategory)) {
            tracer.instant("schedule-" + ioOpName, traceCategory, Scope.p,
                    "{\"path\": \"" + ioOp.getTarget().getRelativePath() + "\"}");
            return new TracedIOOperation(ioOp, tracer, traceCategory);
        }
        return ioOp;
    }

    protected ILSMIOOperation getIoOp() {
        return ioOp;
    }

    @Override
    public IODeviceHandle getDevice() {
        return ioOp.getDevice();
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return ioOp.getCallback();
    }

    @Override
    public String getIndexIdentifier() {
        return ioOp.getIndexIdentifier();
    }

    @Override
    public LSMIOOperationType getIOOpertionType() {
        return ioOpType;
    }

    @Override
    public LSMIOOperationStatus call() throws HyracksDataException {
        final String name = getTarget().getRelativePath();
        final long tid = tracer.durationB(name, traceCategory, null);
        try {
            return ioOp.call();
        } finally {
            tracer.durationE(ioOp.getIOOpertionType().name().toLowerCase(), traceCategory, tid, "{\"size\":"
                    + getTarget().getFile().length() + ", \"path\": \"" + ioOp.getTarget().getRelativePath() + "\"}");
        }
    }

    @Override
    public FileReference getTarget() {
        return ioOp.getTarget();
    }

    @Override
    public ILSMIndexAccessor getAccessor() {
        return ioOp.getAccessor();
    }

    @Override
    public void cleanup(IBufferCache bufferCache) {
        ioOp.cleanup(bufferCache);
    }

    @Override
    public Throwable getFailure() {
        return ioOp.getFailure();
    }

    @Override
    public void setFailure(Throwable failure) {
        ioOp.setFailure(failure);
    }

    @Override
    public LSMIOOperationStatus getStatus() {
        return ioOp.getStatus();
    }

    @Override
    public void setStatus(LSMIOOperationStatus status) {
        ioOp.setStatus(status);
    }

    @Override
    public ILSMDiskComponent getNewComponent() {
        return ioOp.getNewComponent();
    }

    @Override
    public void setNewComponent(ILSMDiskComponent component) {
        ioOp.setNewComponent(component);
    }

    @Override
    public void complete() {
        ioOp.complete();
    }

    @Override
    public void sync() throws InterruptedException {
        ioOp.sync();
    }

    @Override
    public void addCompleteListener(IoOperationCompleteListener listener) {
        ioOp.addCompleteListener(listener);
    }

    @Override
    public Map<String, Object> getParameters() {
        return ioOp.getParameters();
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        ioOp.writeFailed(page, failure);
    }

    @Override
    public boolean hasFailed() {
        return ioOp.hasFailed();
    }

    @Override
    public long getRemainingPages() {
        return ioOp.getRemainingPages();
    }

    @Override
    public void resume() {
        ioOp.resume();
    }

    @Override
    public void pause() {
        ioOp.pause();
    }

    @Override
    public boolean isActive() {
        return ioOp.isActive();
    }
}
