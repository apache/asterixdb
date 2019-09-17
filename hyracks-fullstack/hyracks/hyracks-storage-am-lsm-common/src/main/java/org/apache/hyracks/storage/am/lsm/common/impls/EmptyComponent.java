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

import java.util.Collections;
import java.util.Set;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;

public class EmptyComponent implements ILSMDiskComponent {
    public static final EmptyComponent INSTANCE = new EmptyComponent();

    private EmptyComponent() {
    }

    @Override
    public boolean threadEnter(LSMOperationType opType, boolean isMutableComponent) throws HyracksDataException {
        throw HyracksDataException.create(ErrorCode.ILLEGAL_ATTEMPT_TO_ENTER_EMPTY_COMPONENT);
    }

    @Override
    public void threadExit(LSMOperationType opType, boolean failedOperation, boolean isMutableComponent)
            throws HyracksDataException {
        throw HyracksDataException.create(ErrorCode.ILLEGAL_ATTEMPT_TO_EXIT_EMPTY_COMPONENT);
    }

    @Override
    public ComponentState getState() {
        return ComponentState.INACTIVE;
    }

    @Override
    public ILSMComponentFilter getLSMComponentFilter() {
        return null;
    }

    @Override
    public IIndex getIndex() {
        return null;
    }

    @Override
    public DiskComponentMetadata getMetadata() {
        return EmptyDiskComponentMetadata.INSTANCE;
    }

    @Override
    public long getComponentSize() {
        return 0;
    }

    @Override
    public int getFileReferenceCount() {
        return 0;
    }

    @Override
    public void destroy() throws HyracksDataException {
        // No Op
    }

    @Override
    public ILSMComponentId getId() {
        return LSMComponentId.EMPTY_INDEX_LAST_COMPONENT_ID;
    }

    @Override
    public AbstractLSMIndex getLsmIndex() {
        return null;
    }

    @Override
    public ITreeIndex getMetadataHolder() {
        return null;
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles() {
        return Collections.emptySet();
    }

    @Override
    public void markAsValid(boolean persist, IPageWriteFailureCallback callback) throws HyracksDataException {
        // No Op
    }

    @Override
    public void activate(boolean createNewComponent) throws HyracksDataException {
        // No Op
    }

    @Override
    public void deactivateAndDestroy() throws HyracksDataException {
        // No Op
    }

    @Override
    public void deactivate() throws HyracksDataException {
        // No Op
    }

    @Override
    public void deactivateAndPurge() throws HyracksDataException {
        // No Op
    }

    @Override
    public void validate() throws HyracksDataException {
        // No Op
    }

    @Override
    public ChainedLSMDiskComponentBulkLoader createBulkLoader(ILSMIOOperation operation, float fillFactor,
            boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex, boolean withFilter,
            boolean cleanupEmptyComponent, IPageWriteCallback callback) throws HyracksDataException {
        return null;
    }

    @Override
    public void schedule(LSMIOOperationType ioOperationType) throws HyracksDataException {
        // Do nothing
    }

    @Override
    public int getReaderCount() {
        return 0;
    }
}
