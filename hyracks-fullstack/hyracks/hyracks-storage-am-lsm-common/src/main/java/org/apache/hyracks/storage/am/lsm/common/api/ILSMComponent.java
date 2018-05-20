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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.common.IIndex;

/**
 * An LSM index component. can be an in memory or on disk. Can be readable or unreadable, writable or unwritable
 */
public interface ILSMComponent {

    /**
     * Memory or Disk
     */
    enum LSMComponentType {
        /**
         * Memory component
         */
        MEMORY,
        /**
         * Disk component
         */
        DISK
    }

    /**
     * The state of a component
     */
    enum ComponentState {
        /**
         * The component is inactive (Unreadable and Unwritable). Can be activated
         */
        INACTIVE,
        /**
         * The component can be read from and can be written to
         */
        READABLE_WRITABLE,
        /**
         * Immutable component that can be read from but not written to
         */
        READABLE_UNWRITABLE,
        /**
         * A component that is being flushed. Can be read from but not written to
         */
        READABLE_UNWRITABLE_FLUSHING,
        /**
         * A component that has completed flushing but still has some readers inside
         * This is equivalent to a DEACTIVATING state
         */
        UNREADABLE_UNWRITABLE,
        /**
         * An immutable component that is being merged
         */
        READABLE_MERGING
    }

    /**
     * Enter the component
     *
     * @param opType
     *            the operation over the whole LSM index
     * @param isMutableComponent
     *            true if the thread intends to modify this component (write access), false otherwise
     * @return
     *         true if the thread entered the component, false otherwise
     * @throws HyracksDataException
     *             if the attempted operation is not allowed on the component
     */
    boolean threadEnter(LSMOperationType opType, boolean isMutableComponent) throws HyracksDataException;

    /**
     * Exit the component
     *
     * @param opType
     *            the operation over the whole LSM index under which the thread is running
     * @param failedOperation
     *            whether the operation failed
     * @param isMutableComponent
     *            true if the thread intended to modify the component
     * @throws HyracksDataException
     */
    void threadExit(LSMOperationType opType, boolean failedOperation, boolean isMutableComponent)
            throws HyracksDataException;

    /**
     * @return the component type (memory vs, disk)
     */
    LSMComponentType getType();

    /**
     * @return the component state
     */
    ComponentState getState();

    /**
     * @return the metadata associated with the component
     */
    IComponentMetadata getMetadata();

    /**
     * Note: the component filter is a special case of component metadata in the sense that it is
     * strongly connected with the index and is used by the index logic when doing read and write operations
     * hence, we're leaving this call here
     *
     * @return the component filter
     */
    ILSMComponentFilter getLSMComponentFilter();

    /**
     * @return index data structure that is the stored in the component
     */
    IIndex getIndex();

    /**
     * @return the {@link ILSMIndex} this component belong to
     */
    ILSMIndex getLsmIndex();

    /**
     *
     * @return id of the component
     * @throws HyracksDataException
     */
    ILSMComponentId getId() throws HyracksDataException;

    /**
     * Prepare the component to be scheduled for an IO operation
     *
     * @param ioOperationType
     * @throws HyracksDataException
     */
    void schedule(LSMIOOperationType ioOperationType) throws HyracksDataException;

    /**
     * @return the number of readers inside a component
     */
    int getReaderCount();
}
