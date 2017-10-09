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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public abstract class AbstractIoOperation implements ILSMIOOperation {

    protected final ILSMIndexAccessor accessor;
    protected final FileReference target;
    protected final ILSMIOOperationCallback callback;
    protected final String indexIdentifier;
    protected final List<ILSMIOOperation> dependingOps;

    protected AtomicBoolean isFinished = new AtomicBoolean(false);

    public AbstractIoOperation(ILSMIndexAccessor accessor, FileReference target, ILSMIOOperationCallback callback,
            String indexIdentifier, List<ILSMIOOperation> dependingOps) {
        this.accessor = accessor;
        this.target = target;
        this.callback = callback;
        this.indexIdentifier = indexIdentifier;
        this.dependingOps = dependingOps;
    }

    protected abstract void callInternal() throws HyracksDataException;

    @Override
    public Boolean call() throws HyracksDataException {
        try {
            callInternal();
        } finally {
            synchronized (this) {
                isFinished.set(true);
                notifyAll();
            }
        }
        return true;
    }

    @Override
    public IODeviceHandle getDevice() {
        return target.getDeviceHandle();
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return callback;
    }

    @Override
    public FileReference getTarget() {
        return target;
    }

    @Override
    public ILSMIndexAccessor getAccessor() {
        return accessor;
    }

    @Override
    public String getIndexIdentifier() {
        return indexIdentifier;
    }

    @Override
    public List<ILSMIOOperation> getDependingOps() {
        return dependingOps;
    }

    @Override
    public boolean isFinished() {
        return isFinished.get();
    }
}
