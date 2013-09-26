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

import java.io.File;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IVirtualFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class PartitionedLSMInvertedIndex extends LSMInvertedIndex {

    public PartitionedLSMInvertedIndex(List<IVirtualBufferCache> virtualBufferCaches,
            OnDiskInvertedIndexFactory diskInvIndexFactory, BTreeFactory deletedKeysBTreeFactory,
            BloomFilterFactory bloomFilterFactory, double bloomFilterFalsePositiveRate,
            ILSMIndexFileManager fileManager, IFileMapProvider diskFileMapProvider, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback) throws IndexException {
        super(virtualBufferCaches, diskInvIndexFactory, deletedKeysBTreeFactory, bloomFilterFactory,
                bloomFilterFalsePositiveRate, fileManager, diskFileMapProvider, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, tokenizerFactory, mergePolicy, opTracker, ioScheduler, ioOpCallback);
    }

    @Override
    protected InMemoryInvertedIndex createInMemoryInvertedIndex(IVirtualBufferCache virtualBufferCache,
            IVirtualFreePageManager virtualFreePageManager, int id) throws IndexException {
        return InvertedIndexUtils.createPartitionedInMemoryBTreeInvertedindex(virtualBufferCache,
                virtualFreePageManager, invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories,
                tokenizerFactory, new FileReference(new File(fileManager.getBaseDir() + "_virtual_vocab_" + id)));
    }

}
