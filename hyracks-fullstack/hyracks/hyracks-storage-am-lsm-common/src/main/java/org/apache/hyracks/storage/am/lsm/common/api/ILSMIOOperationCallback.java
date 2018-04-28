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

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ILSMIOOperationCallback {

    /**
     * This method is called on an IO operation before the operation is scheduled
     * For operations that are not scheduled(i,e. Bulk load), this call is skipped.
     *
     * @param operation
     * @throws HyracksDataException
     */
    void scheduled(ILSMIOOperation operation) throws HyracksDataException;

    /**
     * This method is called on an IO operation before the operation starts.
     * (i.e. IO operations could be flush, or merge operations.)
     */
    void beforeOperation(ILSMIOOperation operation) throws HyracksDataException;

    /**
     * This method is called on an IO operation sometime after the operation is completed but before the new component
     * is marked as valid.
     * (i.e. IO operations could be flush or merge operations.)
     *
     * Copying content of metadata page from memory component to disk component should be done in this call
     * Merging content of metadata pages from disk components to new disk component should be done in this call
     *
     * @throws HyracksDataException
     */
    void afterOperation(ILSMIOOperation operation) throws HyracksDataException;

    /**
     * This method is called on an IO operation when the operation needs any cleanup works
     * regardless that the IO operation was executed or not. Once the IO operation is executed,
     * this method should be called after ILSMIOOperationCallback.afterOperation() was called.
     *
     */
    void afterFinalize(ILSMIOOperation operation) throws HyracksDataException;

    /**
     * This method is called after the schduler is done with the IO operation
     * For operation that are not scheduled, this call is skipped
     *
     * @param operation
     * @throws HyracksDataException
     */
    void completed(ILSMIOOperation operation);

    /**
     * This method is called when a memory component is recycled
     *
     * @param component
     */
    void recycled(ILSMMemoryComponent component) throws HyracksDataException;

    /**
     * This method is called when a memory component is allocated
     *
     * @param component
     *            the allocated component
     */
    void allocated(ILSMMemoryComponent component) throws HyracksDataException;
}
