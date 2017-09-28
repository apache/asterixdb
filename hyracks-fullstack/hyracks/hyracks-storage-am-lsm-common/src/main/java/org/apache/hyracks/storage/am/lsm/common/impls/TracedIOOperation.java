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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.util.trace.Tracer;

class TracedIOOperation implements ILSMIOOperation {
    protected final ILSMIOOperation ioOp;
    private final LSMIOOpertionType ioOpType;
    private final Tracer tracer;
    private final String cat;

    protected TracedIOOperation(ILSMIOOperation ioOp, Tracer tracer) {
        this.ioOp = ioOp;
        this.tracer = tracer;
        this.ioOpType = ioOp.getIOOpertionType();
        this.cat = ioOpType.name().toLowerCase();
    }

    public static ILSMIOOperation wrap(final ILSMIOOperation ioOp, final Tracer tracer) {
        if (tracer != null && tracer.isEnabled()) {
            return ioOp instanceof Comparable ? new ComparableTracedIOOperation(ioOp, tracer)
                    : new TracedIOOperation(ioOp, tracer);
        }
        return ioOp;
    }

    protected ILSMIOOperation getIoOp() {
        return ioOp;
    }

    public IODeviceHandle getDevice() {
        return ioOp.getDevice();
    }

    public ILSMIOOperationCallback getCallback() {
        return ioOp.getCallback();
    }

    public String getIndexIdentifier() {
        return ioOp.getIndexIdentifier();
    }

    public LSMIOOpertionType getIOOpertionType() {
        return ioOpType;
    }

    @Override
    public Boolean call() throws HyracksDataException {
        final long tid = tracer.durationB(getDevice().toString(), cat, null);
        try {
            return ioOp.call();
        } finally {
            tracer.durationE(tid, "{\"optional\":\"value\"}");
        }
    }
}

class ComparableTracedIOOperation extends TracedIOOperation implements Comparable<ILSMIOOperation> {

    protected ComparableTracedIOOperation(ILSMIOOperation ioOp, Tracer trace) {
        super(ioOp, trace);
        System.err.println("COMPARE ComparableTracedIOOperation");
    }

    public int compareTo(ILSMIOOperation other) {
        System.err.println("COMPARE compareTo " + other.getClass().getSimpleName());
        if (other instanceof ComparableTracedIOOperation) {
            other = ((ComparableTracedIOOperation) other).getIoOp();
            return ((Comparable) this.ioOp).compareTo(other);
        }
        throw new IllegalArgumentException("Comparing ioOps of type " + this.ioOp.getClass().getSimpleName() + " and "
                + other.getClass().getSimpleName());
    }
}