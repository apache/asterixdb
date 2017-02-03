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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMDiskComponent;

public class LSMBTreeDiskComponent extends AbstractLSMDiskComponent {
    private final BTree btree;
    private final BloomFilter bloomFilter;

    public LSMBTreeDiskComponent(BTree btree, BloomFilter bloomFilter, ILSMComponentFilter filter) {
        super((IMetadataPageManager) btree.getPageManager(), filter);
        this.btree = btree;
        this.bloomFilter = bloomFilter;
    }

    @Override
    public void destroy() throws HyracksDataException {
        btree.deactivate();
        btree.destroy();
        if (bloomFilter != null) {
            bloomFilter.deactivate();
            bloomFilter.destroy();
        }
    }

    public BTree getBTree() {
        return btree;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public long getComponentSize() {
        return btree.getFileReference().getFile().length()
                + (bloomFilter == null ? 0 : bloomFilter.getFileReference().getFile().length());
    }

    @Override
    public int getFileReferenceCount() {
        return btree.getBufferCache().getFileReferenceCount(btree.getFileId());
    }
}
