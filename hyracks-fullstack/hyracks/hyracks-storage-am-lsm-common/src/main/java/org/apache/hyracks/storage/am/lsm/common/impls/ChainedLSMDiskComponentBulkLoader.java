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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.util.annotations.CriticalPath;

/**
 * Class encapsulates a chain of operations, happening during an LSM disk component bulkload
 */
public class ChainedLSMDiskComponentBulkLoader implements ILSMDiskComponentBulkLoader {

    private static final int CHECK_CYCLE = 1000;

    private List<IChainedComponentBulkLoader> bulkloaderChain = new ArrayList<>();
    private final ILSMIOOperation operation;
    private final ILSMDiskComponent diskComponent;
    private final boolean cleanupEmptyComponent;
    private boolean isEmptyComponent = true;
    private boolean cleanedUpArtifacts = false;
    private int tupleCounter = 0;

    public ChainedLSMDiskComponentBulkLoader(ILSMIOOperation operation, ILSMDiskComponent diskComponent,
            boolean cleanupEmptyComponent) {
        this.operation = operation;
        this.diskComponent = diskComponent;
        this.cleanupEmptyComponent = cleanupEmptyComponent;
    }

    public void addBulkLoader(IChainedComponentBulkLoader bulkloader) {
        bulkloaderChain.add(bulkloader);
    }

    @SuppressWarnings("squid:S1181")
    @Override
    @CriticalPath
    public void add(ITupleReference tuple) throws HyracksDataException {
        try {
            ITupleReference t = tuple;
            final int bulkloadersCount = bulkloaderChain.size();
            for (int i = 0; i < bulkloadersCount; i++) {
                t = bulkloaderChain.get(i).add(t);
            }
            checkOperation();
        } catch (Throwable e) {
            operation.setFailure(e);
            cleanupArtifacts();
            throw e;
        }
        if (isEmptyComponent) {
            isEmptyComponent = false;
        }
    }

    @SuppressWarnings("squid:S1181")
    @Override
    @CriticalPath
    public void delete(ITupleReference tuple) throws HyracksDataException {
        try {
            ITupleReference t = tuple;
            final int bulkloadersCount = bulkloaderChain.size();
            for (int i = 0; i < bulkloadersCount; i++) {
                t = bulkloaderChain.get(i).delete(t);
            }
            checkOperation();
        } catch (Throwable e) {
            operation.setFailure(e);
            cleanupArtifacts();
            throw e;
        }
        if (isEmptyComponent) {
            isEmptyComponent = false;
        }
    }

    @Override
    public void cleanupArtifacts() throws HyracksDataException {
        if (!cleanedUpArtifacts) {
            cleanedUpArtifacts = true;
            final int bulkloadersCount = bulkloaderChain.size();
            for (int i = 0; i < bulkloadersCount; i++) {
                bulkloaderChain.get(i).cleanupArtifacts();;
            }
            diskComponent.deactivateAndDestroy();
        }
    }

    @Override
    public void end() throws HyracksDataException {
        if (!cleanedUpArtifacts) {
            final int bulkloadersCount = bulkloaderChain.size();
            for (int i = 0; i < bulkloadersCount; i++) {
                bulkloaderChain.get(i).end();
            }
            if (isEmptyComponent && cleanupEmptyComponent) {
                cleanupArtifacts();
            }
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        operation.setStatus(LSMIOOperationStatus.FAILURE);
        final int bulkloadersCount = bulkloaderChain.size();
        for (int i = 0; i < bulkloadersCount; i++) {
            bulkloaderChain.get(i).abort();
        }
    }

    @Override
    public ILSMIOOperation getOperation() {
        return operation;
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFailed() {
        final int bulkloadersCount = bulkloaderChain.size();
        for (int i = 0; i < bulkloadersCount; i++) {
            if (bulkloaderChain.get(i).hasFailed()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Throwable getFailure() {
        final int bulkloadersCount = bulkloaderChain.size();
        for (int i = 0; i < bulkloadersCount; i++) {
            if (bulkloaderChain.get(i).hasFailed()) {
                return bulkloaderChain.get(i).getFailure();
            }
        }
        return null;
    }

    @Override
    public void force() throws HyracksDataException {
        for (IChainedComponentBulkLoader bulkLoader : bulkloaderChain) {
            bulkLoader.force();
        }
    }

    private void checkOperation() throws HyracksDataException {
        if (operation.getIOOpertionType() == LSMIOOperationType.MERGE && ++tupleCounter % CHECK_CYCLE == 0) {
            tupleCounter = 0;
            ((MergeOperation) operation).waitIfPaused();
        }
    }
}
