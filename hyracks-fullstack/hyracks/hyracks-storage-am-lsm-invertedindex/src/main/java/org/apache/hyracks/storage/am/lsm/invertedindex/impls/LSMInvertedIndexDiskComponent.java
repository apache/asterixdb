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
package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.lsm.common.api.AbstractLSMWithBuddyDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.IChainedComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexWithBuddyBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.util.ComponentUtils;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;

public class LSMInvertedIndexDiskComponent extends AbstractLSMWithBuddyDiskComponent {

    private final OnDiskInvertedIndex invIndex;
    private final BTree deletedKeysBTree;
    private final BloomFilter bloomFilter;

    public LSMInvertedIndexDiskComponent(AbstractLSMIndex lsmIndex, OnDiskInvertedIndex invIndex,
            BTree deletedKeysBTree, BloomFilter bloomFilter, ILSMComponentFilter filter) {
        super(lsmIndex, (IMetadataPageManager) deletedKeysBTree.getPageManager(), filter);
        this.invIndex = invIndex;
        this.deletedKeysBTree = deletedKeysBTree;
        this.bloomFilter = bloomFilter;
    }

    @Override
    public OnDiskInvertedIndex getIndex() {
        return invIndex;
    }

    @Override
    public BTree getMetadataHolder() {
        return invIndex.getBTree();
    }

    @Override
    public BTree getBuddyIndex() {
        return deletedKeysBTree;
    }

    @Override
    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public IBufferCache getBloomFilterBufferCache() {
        return invIndex.getBufferCache();
    }

    @Override
    public long getComponentSize() {
        return invIndex.getInvListsFile().getFile().length() + invIndex.getBTree().getFileReference().getFile().length()
                + deletedKeysBTree.getFileReference().getFile().length()
                + bloomFilter.getFileReference().getFile().length();
    }

    @Override
    public int getFileReferenceCount() {
        return deletedKeysBTree.getBufferCache().getFileReferenceCount(deletedKeysBTree.getFileId());
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles() {
        Set<String> files = new HashSet<>();
        files.add(invIndex.getInvListsFile().getFile().getAbsolutePath());
        files.add(invIndex.getBTree().getFileReference().getFile().getAbsolutePath());
        files.add(bloomFilter.getFileReference().getFile().getAbsolutePath());
        files.add(deletedKeysBTree.getFileReference().getFile().getAbsolutePath());
        return files;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + invIndex.getInvListsFile().getRelativePath();
    }

    @Override
    public void markAsValid(boolean persist, IPageWriteFailureCallback callback) throws HyracksDataException {
        ComponentUtils.markAsValid(getBloomFilterBufferCache(), getBloomFilter(), persist);

        // Flush inverted index second.
        invIndex.getBufferCache().force((invIndex).getInvListsFileId(), true);
        ComponentUtils.markAsValid(getMetadataHolder(), persist, callback);
        if (!callback.hasFailed()) {
            // Flush deleted keys BTree.
            ComponentUtils.markAsValid(getBuddyIndex(), persist, callback);
        }
        if (callback.hasFailed()) {
            throw HyracksDataException.create(callback.getFailure());
        }
    }

    @Override
    protected IChainedComponentBulkLoader createMergeIndexBulkLoader(float fillFactor, boolean verifyInput,
            long numElementsHint, boolean checkIfEmptyIndex, IPageWriteCallback callback) throws HyracksDataException {
        IIndexBulkLoader indexBulkLoader =
                invIndex.createMergeBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex, callback);
        IIndexBulkLoader buddyBulkLoader =
                getBuddyIndex().createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex, callback);
        return new IndexWithBuddyBulkLoader(indexBulkLoader, buddyBulkLoader);
    }
}
