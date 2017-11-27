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

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;

public interface ILSMIOOperationCallback {

    /**
     * This method is called on an IO operation before the operation starts.
     * (i.e. IO operations could be flush or merge operations.)
     * For flush, this is called immediately before switching the current memory component pointer
     */
    void beforeOperation(LSMIOOperationType opType) throws HyracksDataException;

    /**
     * This method is called on an IO operation sometime after the operation was completed.
     * (i.e. IO operations could be flush or merge operations.)
     *
     * Copying content of metadata page from memory component to disk component should be done in this call
     * Merging content of metadata pages from disk components to new disk component should be done in this call
     *
     * @param oldComponents
     * @param newComponent
     * @throws HyracksDataException
     */
    void afterOperation(LSMIOOperationType opType, List<ILSMComponent> oldComponents, ILSMDiskComponent newComponent)
            throws HyracksDataException;

    /**
     * This method is called on an IO operation when the operation needs any cleanup works
     * regardless that the IO operation was executed or not. Once the IO operation is executed,
     * this method should be called after ILSMIOOperationCallback.afterOperation() was called.
     *
     * @param newComponent
     * @throws HyracksDataException
     */
    void afterFinalize(LSMIOOperationType opType, ILSMDiskComponent newComponent) throws HyracksDataException;

    /**
     * This method is called when a memory component is recycled
     *
     * @param component
     * @param componentSwitched
     *            true if the component index was advanced for this recycle, false otherwise
     */
    void recycled(ILSMMemoryComponent component, boolean componentSwitched) throws HyracksDataException;

    /**
     * This method is called when a memory component is allocated
     *
     * @param component
     */
    void allocated(ILSMMemoryComponent component) throws HyracksDataException;

}
