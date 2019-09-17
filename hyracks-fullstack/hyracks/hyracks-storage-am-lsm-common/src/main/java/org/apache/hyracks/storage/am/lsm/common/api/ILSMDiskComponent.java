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

import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;

public interface ILSMDiskComponent extends ILSMComponent {

    @Override
    default LSMComponentType getType() {
        return LSMComponentType.DISK;
    }

    @Override
    DiskComponentMetadata getMetadata();

    /**
     * @return the on disk size of this component
     */
    long getComponentSize();

    /**
     * @return the reference count for the component
     */
    int getFileReferenceCount();

    /**
     * @return LsmIndex of the component
     */
    @Override
    AbstractLSMIndex getLsmIndex();

    /**
     * @return the TreeIndex which holds metadata for the disk component
     */
    ITreeIndex getMetadataHolder();

    /**
     * @return a set of files describing the contents of the disk component
     */
    Set<String> getLSMComponentPhysicalFiles();

    /**
     * Mark the component as valid
     *
     * @param persist
     *            whether the call should force data to disk before returning
     * @param callback
     *            callback for when a page write operation fails
     * @throws HyracksDataException
     */
    void markAsValid(boolean persist, IPageWriteFailureCallback callback) throws HyracksDataException;

    /**
     * Activates the component
     *
     * @param create
     *            whether a new component should be created
     * @throws HyracksDataException
     */
    void activate(boolean create) throws HyracksDataException;

    /**
     * Deactivate and destroy the component (Deletes it from disk)
     *
     * @throws HyracksDataException
     */
    void deactivateAndDestroy() throws HyracksDataException;

    /**
     * Destroy the component (Deletes it from disk)
     *
     * @throws HyracksDataException
     */
    void destroy() throws HyracksDataException;

    /**
     * Deactivate the component
     * The pages are still in the buffer cache
     *
     * @throws HyracksDataException
     */
    void deactivate() throws HyracksDataException;

    /**
     * Deactivate the component and purge it out of the buffer cache
     *
     * @throws HyracksDataException
     */
    void deactivateAndPurge() throws HyracksDataException;

    /**
     * Test method. validates the content of the component
     * TODO: Remove this method from the interface
     *
     * @throws HyracksDataException
     */
    void validate() throws HyracksDataException;

    /**
     * Creates a bulkloader pipeline which includes all chained operations, bulkloading individual elements of the
     * component: indexes, LSM filters, Bloom filters, buddy indexes, etc.
     *
     * @param operation
     * @param fillFactor
     * @param verifyInput
     * @param numElementsHint
     * @param checkIfEmptyIndex
     * @param withFilter
     * @param cleanupEmptyComponent
     * @return the created disk component bulk loader
     * @throws HyracksDataException
     */
    ILSMDiskComponentBulkLoader createBulkLoader(ILSMIOOperation operation, float fillFactor, boolean verifyInput,
            long numElementsHint, boolean checkIfEmptyIndex, boolean withFilter, boolean cleanupEmptyComponent,
            IPageWriteCallback callback) throws HyracksDataException;
}
