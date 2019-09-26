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

public class NoOpIoOperation implements ILSMIOOperation {
    public static final NoOpIoOperation INSTANCE = new NoOpIoOperation();

    private NoOpIoOperation() {
    }

    @Override
    public IODeviceHandle getDevice() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getIndexIdentifier() {
        return NoOpIoOperation.class.getSimpleName();
    }

    @Override
    public LSMIOOperationType getIOOpertionType() {
        return LSMIOOperationType.NOOP;
    }

    @Override
    public LSMIOOperationStatus call() throws HyracksDataException {
        return LSMIOOperationStatus.SUCCESS;
    }

    @Override
    public FileReference getTarget() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILSMIndexAccessor getAccessor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanup(IBufferCache bufferCache) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Throwable getFailure() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFailure(Throwable failure) {
        // No Op
    }

    @Override
    public LSMIOOperationStatus getStatus() {
        return LSMIOOperationStatus.SUCCESS;
    }

    @Override
    public void setStatus(LSMIOOperationStatus status) {
        // No Op
    }

    @Override
    public ILSMDiskComponent getNewComponent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNewComponent(ILSMDiskComponent component) {
        // No Op
    }

    @Override
    public void complete() {
        // No Op
    }

    @Override
    public void sync() {
        // No Op
    }

    @Override
    public void addCompleteListener(IoOperationCompleteListener listener) {
        listener.completed(this);
    }

    @Override
    public Map<String, Object> getParameters() {
        return null;
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFailed() {
        return false;
    }

    @Override
    public long getRemainingPages() {
        return 0;
    }

    @Override
    public void resume() {
        // No Op
    }

    @Override
    public void pause() {
        // No Op
    }

    @Override
    public boolean isActive() {
        return false;
    }

}
