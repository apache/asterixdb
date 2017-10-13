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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;

/**
 * Class encapsulates a chain of operations, happening during an LSM disk component bulkload
 */
public class ChainedLSMDiskComponentBulkLoader implements ILSMDiskComponentBulkLoader {

    private List<IChainedComponentBulkLoader> bulkloaderChain = new LinkedList<>();
    private boolean isEmptyComponent = true;
    private boolean cleanedUpArtifacts = false;
    private final ILSMDiskComponent diskComponent;
    private final boolean cleanupEmptyComponent;

    public ChainedLSMDiskComponentBulkLoader(ILSMDiskComponent diskComponent, boolean cleanupEmptyComponent) {
        this.diskComponent = diskComponent;
        this.cleanupEmptyComponent = cleanupEmptyComponent;
    }

    public void addBulkLoader(IChainedComponentBulkLoader bulkloader) {
        bulkloaderChain.add(bulkloader);
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        try {
            ITupleReference t = tuple;
            for (IChainedComponentBulkLoader lsmBulkloader : bulkloaderChain) {
                t = lsmBulkloader.add(t);
            }
        } catch (Exception e) {
            cleanupArtifacts();
            throw e;
        }
        if (isEmptyComponent) {
            isEmptyComponent = false;
        }
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException {
        try {
            ITupleReference t = tuple;
            for (IChainedComponentBulkLoader lsmOperation : bulkloaderChain) {
                t = lsmOperation.delete(t);
            }
        } catch (Exception e) {
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
            for (IChainedComponentBulkLoader lsmOperation : bulkloaderChain) {
                lsmOperation.cleanupArtifacts();
            }
        }
        diskComponent.deactivateAndDestroy();
    }

    @Override
    public void end() throws HyracksDataException {
        if (!cleanedUpArtifacts) {
            for (IChainedComponentBulkLoader lsmOperation : bulkloaderChain) {
                lsmOperation.end();
            }
            if (isEmptyComponent && cleanupEmptyComponent) {
                cleanupArtifacts();
            }
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        for (IChainedComponentBulkLoader lsmOperation : bulkloaderChain) {
            lsmOperation.abort();
        }
    }
}
