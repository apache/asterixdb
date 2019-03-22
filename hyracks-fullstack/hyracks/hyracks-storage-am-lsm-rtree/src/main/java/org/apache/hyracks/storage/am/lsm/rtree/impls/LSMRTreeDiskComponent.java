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
package org.apache.hyracks.storage.am.lsm.rtree.impls;

import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.lsm.common.api.AbstractLSMWithBuddyDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class LSMRTreeDiskComponent extends AbstractLSMWithBuddyDiskComponent {
    private final RTree rtree;
    private final BTree btree;
    private final BloomFilter bloomFilter;

    public LSMRTreeDiskComponent(AbstractLSMIndex lsmIndex, RTree rtree, BTree btree, BloomFilter bloomFilter,
            ILSMComponentFilter filter) {
        super(lsmIndex, getMetadataPageManager(rtree), filter);
        this.rtree = rtree;
        this.btree = btree;
        this.bloomFilter = bloomFilter;
    }

    @Override
    public BTree getBuddyIndex() {
        return btree;
    }

    @Override
    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public IBufferCache getBloomFilterBufferCache() {
        return btree.getBufferCache();
    }

    @Override
    public long getComponentSize() {
        long size = getComponentSize(rtree);
        size += btree.getFileReference().getFile().length();
        size += bloomFilter.getFileReference().getFile().length();
        return size;
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles() {
        Set<String> files = getFiles(rtree);
        files.add(btree.getFileReference().getFile().getAbsolutePath());
        files.add(bloomFilter.getFileReference().getFile().getAbsolutePath());
        return files;
    }

    @Override
    public int getFileReferenceCount() {
        return getFileReferenceCount(rtree);
    }

    @Override
    public RTree getMetadataHolder() {
        return rtree;
    }

    @Override
    public RTree getIndex() {
        return rtree;
    }

    static IMetadataPageManager getMetadataPageManager(RTree rtree) {
        return (IMetadataPageManager) rtree.getPageManager();
    }

    static long getComponentSize(RTree rtree) {
        return rtree.getFileReference().getFile().length();
    }

    static int getFileReferenceCount(RTree rtree) {
        return rtree.getBufferCache().getFileReferenceCount(rtree.getFileId());
    }

    static Set<String> getFiles(RTree rtree) {
        Set<String> files = new HashSet<>();
        files.add(rtree.getFileReference().getFile().getAbsolutePath());
        return files;
    }
}
