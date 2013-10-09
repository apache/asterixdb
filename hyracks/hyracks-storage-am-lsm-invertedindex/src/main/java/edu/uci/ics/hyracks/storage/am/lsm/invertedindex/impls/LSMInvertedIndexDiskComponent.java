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
package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractDiskLSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;

public class LSMInvertedIndexDiskComponent extends AbstractDiskLSMComponent {

    private final IInvertedIndex invIndex;
    private final BTree deletedKeysBTree;
    private final BloomFilter bloomFilter;

    public LSMInvertedIndexDiskComponent(IInvertedIndex invIndex, BTree deletedKeysBTree, BloomFilter bloomFilter) {
        this.invIndex = invIndex;
        this.deletedKeysBTree = deletedKeysBTree;
        this.bloomFilter = bloomFilter;
    }

    @Override
    public void destroy() throws HyracksDataException {
        invIndex.deactivate();
        invIndex.destroy();
        deletedKeysBTree.deactivate();
        deletedKeysBTree.destroy();
        bloomFilter.deactivate();
        bloomFilter.destroy();
    }

    public IInvertedIndex getInvIndex() {
        return invIndex;
    }

    public BTree getDeletedKeysBTree() {
        return deletedKeysBTree;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public long getComponentSize() {
        return ((OnDiskInvertedIndex) invIndex).getInvListsFile().getFile().length()
                + ((OnDiskInvertedIndex) invIndex).getBTree().getFileReference().getFile().length()
                + deletedKeysBTree.getFileReference().getFile().length()
                + bloomFilter.getFileReference().getFile().length();
    }
}
