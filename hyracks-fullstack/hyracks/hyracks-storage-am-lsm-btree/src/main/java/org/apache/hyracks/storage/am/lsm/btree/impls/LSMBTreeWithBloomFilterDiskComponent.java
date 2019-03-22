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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.util.Set;

import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.api.AbstractLSMWithBloomFilterDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class LSMBTreeWithBloomFilterDiskComponent extends AbstractLSMWithBloomFilterDiskComponent {

    private final BTree btree;
    private final BloomFilter bloomFilter;

    public LSMBTreeWithBloomFilterDiskComponent(AbstractLSMIndex lsmIndex, BTree btree, BloomFilter bloomFilter,
            ILSMComponentFilter filter) {
        super(lsmIndex, LSMBTreeDiskComponent.getMetadataPageManager(btree), filter);
        this.btree = btree;
        this.bloomFilter = bloomFilter;
    }

    @Override
    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public IBufferCache getBloomFilterBufferCache() {
        return getMetadataHolder().getBufferCache();
    }

    @Override
    public long getComponentSize() {
        return LSMBTreeDiskComponent.getComponentSize(btree) + getComponentSize(bloomFilter);
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles() {
        Set<String> files = LSMBTreeDiskComponent.getFiles(btree);
        addFiles(files, bloomFilter);
        return files;
    }

    static void addFiles(Set<String> files, BloomFilter bloomFilter) {
        files.add(bloomFilter.getFileReference().getFile().getAbsolutePath());
    }

    @Override
    public int getFileReferenceCount() {
        return LSMBTreeDiskComponent.getFileReferenceCount(btree);
    }

    @Override
    public BTree getMetadataHolder() {
        return btree;
    }

    @Override
    public BTree getIndex() {
        return btree;
    }

    static long getComponentSize(BloomFilter bloomFilter) {
        return bloomFilter.getFileReference().getFile().length();
    }
}
