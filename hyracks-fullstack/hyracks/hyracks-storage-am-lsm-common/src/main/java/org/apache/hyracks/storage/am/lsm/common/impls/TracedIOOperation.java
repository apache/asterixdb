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

import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.hyracks.util.trace.ITracer.Scope;

class TracedIOOperation implements ILSMIOOperation {

    static final Logger LOGGER = Logger.getLogger(TracedIOOperation.class.getName());

    protected final ILSMIOOperation ioOp;
    private final LSMIOOpertionType ioOpType;
    private final ITracer tracer;
    private final String cat;

    protected TracedIOOperation(ILSMIOOperation ioOp, ITracer tracer) {
        this.ioOp = ioOp;
        this.tracer = tracer;
        this.ioOpType = ioOp.getIOOpertionType();
        this.cat = ioOpType.name().toLowerCase();
    }

    public static ILSMIOOperation wrap(final ILSMIOOperation ioOp, final ITracer tracer) {
        if (tracer != null && tracer.isEnabled()) {
            tracer.instant(ioOp.getTarget().getRelativePath(),
                    ioOp.getIOOpertionType() == LSMIOOpertionType.FLUSH ? "schedule-flush" : "schedule-merge", Scope.p,
                    null);
            return ioOp instanceof Comparable ? new ComparableTracedIOOperation(ioOp, tracer)
                    : new TracedIOOperation(ioOp, tracer);
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
    public LSMIOOpertionType getIOOpertionType() {
        return ioOpType;
    }

    @Override
    public Boolean call() throws HyracksDataException {
        final long tid = tracer.durationB(getTarget().getRelativePath(), cat, null);
        try {
            return ioOp.call();
        } finally {
            tracer.durationE(tid, "{\"optional\":\"value\"}");
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
}

class ComparableTracedIOOperation extends TracedIOOperation implements Comparable<ILSMIOOperation> {

    protected ComparableTracedIOOperation(ILSMIOOperation ioOp, ITracer trace) {
        super(ioOp, trace);
    }

    @Override
    public int hashCode() {
        return this.ioOp.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ILSMIOOperation && compareTo((ILSMIOOperation) other) == 0;
    }

    @Override
    public int compareTo(ILSMIOOperation other) {
        final ILSMIOOperation myIoOp = this.ioOp;
        if (myIoOp instanceof Comparable && other instanceof ComparableTracedIOOperation) {
            return ((Comparable) myIoOp).compareTo(((ComparableTracedIOOperation) other).getIoOp());
        }
        LOGGER.warning("Comparing ioOps of type " + myIoOp.getClass().getSimpleName() + " and "
                + other.getClass().getSimpleName() + " in " + getClass().getSimpleName());
        return Integer.signum(hashCode() - other.hashCode());
    }
}