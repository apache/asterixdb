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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.IoOperationCompleteListener;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public abstract class AbstractIoOperation implements ILSMIOOperation {

    protected final ILSMIndexAccessor accessor;
    protected final FileReference target;
    protected final ILSMIOOperationCallback callback;
    protected final String indexIdentifier;
    private volatile Throwable failure;
    private LSMIOOperationStatus status = LSMIOOperationStatus.SUCCESS;
    private ILSMDiskComponent newComponent;
    private boolean completed = false;
    private List<IoOperationCompleteListener> completeListeners;

    public AbstractIoOperation(ILSMIndexAccessor accessor, FileReference target, ILSMIOOperationCallback callback,
            String indexIdentifier) {
        this.accessor = accessor;
        this.target = target;
        this.callback = callback;
        this.indexIdentifier = indexIdentifier;
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
    public Throwable getFailure() {
        return failure;
    }

    @Override
    public void setFailure(Throwable failure) {
        status = LSMIOOperationStatus.FAILURE;
        this.failure = ExceptionUtils.suppress(this.failure, failure);
    }

    @Override
    public LSMIOOperationStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(LSMIOOperationStatus status) {
        this.status = status;
    }

    @Override
    public ILSMDiskComponent getNewComponent() {
        return newComponent;
    }

    @Override
    public void setNewComponent(ILSMDiskComponent component) {
        this.newComponent = component;
    }

    @Override
    public synchronized void complete() {
        if (completed) {
            throw new IllegalStateException("Multiple destroy calls");
        }
        callback.completed(this);
        completed = true;
        if (completeListeners != null) {
            for (IoOperationCompleteListener listener : completeListeners) {
                listener.completed(this);
            }
            completeListeners = null;
        }
        notifyAll();
    }

    @Override
    public synchronized void sync() throws InterruptedException {
        while (!completed) {
            wait();
        }
    }

    @Override
    public Map<String, Object> getParameters() {
        return accessor.getOpContext().getParameters();
    }

    @Override
    public synchronized void addCompleteListener(IoOperationCompleteListener listener) {
        if (completed) {
            listener.completed(this);
        } else {
            if (completeListeners == null) {
                completeListeners = new LinkedList<>();
            }
            completeListeners.add(listener);
        }
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        setFailure(failure);
    }

    @Override
    public boolean hasFailed() {
        return status == LSMIOOperationStatus.FAILURE;
    }
}
