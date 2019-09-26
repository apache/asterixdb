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
package org.apache.hyracks.storage.am.lsm.common.api;

import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;

public interface ILSMIOOperation extends Callable<LSMIOOperationStatus>, IPageWriteFailureCallback {

    /**
     * Represents the io operation type
     */
    enum LSMIOOperationType {
        FLUSH,
        MERGE,
        LOAD,
        NOOP
    }

    /**
     * Represents the status of the IO operation
     */
    enum LSMIOOperationStatus {
        /**
         * Operation successful
         */
        SUCCESS,
        /**
         * Operation failed
         */
        FAILURE
    }

    /**
     * @return the device on which the operation is running
     */
    IODeviceHandle getDevice();

    /**
     * @return the operation callback
     */
    ILSMIOOperationCallback getCallback();

    /**
     * @return the index id
     */
    String getIndexIdentifier();

    /**
     * @return the operation type
     */
    LSMIOOperationType getIOOpertionType();

    @Override
    LSMIOOperationStatus call() throws HyracksDataException;

    /**
     * @return The target of the io operation
     */
    FileReference getTarget();

    /**
     * @return the accessor of the operation
     */
    ILSMIndexAccessor getAccessor();

    /**
     * clean up left over files in case of an exception during execution
     *
     * @param bufferCache
     *            a buffercache that manages the files
     */
    void cleanup(IBufferCache bufferCache);

    /**
     * @return the failure in the io operation if any, null otherwise
     */
    @Override
    Throwable getFailure();

    /**
     * @return set the failure in the io operation
     */
    void setFailure(Throwable failure);

    /**
     * @return the status of the IO operation
     */
    LSMIOOperationStatus getStatus();

    /**
     * Set the status of the IO operation
     *
     * @param status
     */
    void setStatus(LSMIOOperationStatus status);

    /**
     * @return the new component produced by this operation if any, null otherwise
     */
    ILSMDiskComponent getNewComponent();

    /**
     * Set the new component produced by this operation
     *
     * @param component
     */
    void setNewComponent(ILSMDiskComponent component);

    /**
     * Destroy the operation after the scheduler is done with it
     */
    void complete();

    /**
     * Wait for the operation to complete
     *
     * @throws InterruptedException
     */
    void sync() throws InterruptedException;

    /**
     * Add a listener for operation complete event
     *
     * @param listener
     */
    void addCompleteListener(IoOperationCompleteListener listener);

    /**
     * Get parameters passed when calling this IO operation
     */
    Map<String, Object> getParameters();

    /**
     *
     * @return the estimated number of disk pages remaining for this IO operation
     */
    long getRemainingPages();

    /**
     * Resume this IO operation
     */
    void resume();

    /**
     * Pause this IO operation
     */
    void pause();

    /**
     *
     * @return whether this IO operation is currently active (i.e., not paused)
     */
    boolean isActive();

}
