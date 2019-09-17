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
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.BloomFilterBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.ChainedLSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.IChainedComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.util.ComponentUtils;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;

public abstract class AbstractLSMWithBloomFilterDiskComponent extends AbstractLSMDiskComponent {
    public AbstractLSMWithBloomFilterDiskComponent(AbstractLSMIndex lsmIndex, IMetadataPageManager mdPageManager,
            ILSMComponentFilter filter) {
        super(lsmIndex, mdPageManager, filter);
    }

    public abstract BloomFilter getBloomFilter();

    public abstract IBufferCache getBloomFilterBufferCache();

    @Override
    public void markAsValid(boolean persist, IPageWriteFailureCallback callback) throws HyracksDataException {
        // The order of forcing the dirty page to be flushed is critical. The
        // bloom filter must be always done first.
        ComponentUtils.markAsValid(getBloomFilterBufferCache(), getBloomFilter(), persist);
        super.markAsValid(persist, callback);
    }

    @Override
    public void activate(boolean createNewComponent) throws HyracksDataException {
        super.activate(createNewComponent);
        if (createNewComponent) {
            getBloomFilter().create();
        }
        getBloomFilter().activate();
    }

    @Override
    public void destroy() throws HyracksDataException {
        super.destroy();
        getBloomFilter().destroy();
    }

    @Override
    public void deactivate() throws HyracksDataException {
        super.deactivate();
        getBloomFilter().deactivate();
    }

    @Override
    protected void purge() throws HyracksDataException {
        super.purge();
        getBloomFilter().purge();
    }

    public IChainedComponentBulkLoader createBloomFilterBulkLoader(long numElementsHint, IPageWriteCallback callback)
            throws HyracksDataException {
        BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(
                BloomCalculations.maxBucketsPerElement(numElementsHint), getLsmIndex().bloomFilterFalsePositiveRate());
        return new BloomFilterBulkLoader(getBloomFilter().createBuilder(numElementsHint, bloomFilterSpec.getNumHashes(),
                bloomFilterSpec.getNumBucketsPerElements(), callback));
    }

    @Override
    public ChainedLSMDiskComponentBulkLoader createBulkLoader(ILSMIOOperation operation, float fillFactor,
            boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex, boolean withFilter,
            boolean cleanupEmptyComponent, IPageWriteCallback callback) throws HyracksDataException {
        ChainedLSMDiskComponentBulkLoader chainedBulkLoader = super.createBulkLoader(operation, fillFactor, verifyInput,
                numElementsHint, checkIfEmptyIndex, withFilter, cleanupEmptyComponent, callback);
        if (numElementsHint > 0) {
            chainedBulkLoader.addBulkLoader(createBloomFilterBulkLoader(numElementsHint, callback));
        }
        return chainedBulkLoader;
    }
}
