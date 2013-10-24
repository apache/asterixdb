/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractDiskLSMComponent;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;

public class LSMRTreeDiskComponent extends AbstractDiskLSMComponent {
    private final RTree rtree;
    private final BTree btree;
    private final BloomFilter bloomFilter;

    public LSMRTreeDiskComponent(RTree rtree, BTree btree, BloomFilter bloomFilter) {
        this.rtree = rtree;
        this.btree = btree;
        this.bloomFilter = bloomFilter;
    }

    @Override
    public void destroy() throws HyracksDataException {
        rtree.deactivate();
        rtree.destroy();
        if (btree != null) {
            btree.deactivate();
            btree.destroy();
            bloomFilter.deactivate();
            bloomFilter.destroy();
        }
    }

    public RTree getRTree() {
        return rtree;
    }

    public BTree getBTree() {
        return btree;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public long getComponentSize() {
        long size = rtree.getFileReference().getFile().length();
        if (btree != null) {
            size += btree.getFileReference().getFile().length();
            size += bloomFilter.getFileReference().getFile().length();
        }
        return size;
    }
}
