/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOperatorDescriptorHelper;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexRegistry;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexOperatorDescriptorHelper;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public final class InvertedIndexOpHelper {

    private InvertedIndex invIndex;
    private int invIndexFileId = -1;
    private int partition;

    private IInvertedIndexOperatorDescriptorHelper opDesc;
    private IHyracksTaskContext ctx;

    private IndexHelperOpenMode mode;

    public InvertedIndexOpHelper(IInvertedIndexOperatorDescriptorHelper opDesc, final IHyracksTaskContext ctx,
            int partition, IndexHelperOpenMode mode) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.mode = mode;
        this.partition = partition;
    }

    public void init() throws HyracksDataException {
        IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
        IFileMapProvider fileMapProvider = opDesc.getStorageManager().getFileMapProvider(ctx);
        IFileSplitProvider fileSplitProvider = opDesc.getInvIndexFileSplitProvider();

        FileReference f = fileSplitProvider.getFileSplits()[partition].getLocalFile();
        boolean fileIsMapped = fileMapProvider.isMapped(f);

        switch (mode) {

            case OPEN: {
                if (!fileIsMapped) {
                    throw new HyracksDataException("Trying to open inverted index from unmapped file " + f.toString());
                }
            }
                break;

            case CREATE:
            case ENLIST: {
                if (!fileIsMapped) {
                    bufferCache.createFile(f);
                }
            }
                break;

        }

        int fileId = fileMapProvider.lookupFileId(f);
        try {
            bufferCache.openFile(fileId);
        } catch (HyracksDataException e) {
            // revert state of buffer cache since file failed to open
            if (!fileIsMapped) {
                bufferCache.deleteFile(fileId);
            }
            throw e;
        }

        // only set btreeFileId member when openFile() succeeds,
        // otherwise deinit() will try to close the file that failed to open
        invIndexFileId = fileId;
        IndexRegistry<InvertedIndex> invIndexRegistry = opDesc.getInvIndexRegistryProvider().getRegistry(ctx);
        invIndex = invIndexRegistry.get(invIndexFileId);
        if (invIndex == null) {

            // create new inverted index and register it
            invIndexRegistry.lock();
            try {
                // check if inverted index has already been registered by
                // another thread
                invIndex = invIndexRegistry.get(invIndexFileId);
                if (invIndex == null) {
                    // this thread should create and register the inverted index

                    IBinaryComparator[] comparators = new IBinaryComparator[opDesc.getInvIndexComparatorFactories().length];
                    for (int i = 0; i < opDesc.getInvIndexComparatorFactories().length; i++) {
                        comparators[i] = opDesc.getInvIndexComparatorFactories()[i].createBinaryComparator();
                    }

                    MultiComparator cmp = new MultiComparator(comparators);

                    // assumes btree has already been registered
                    IFileSplitProvider btreeFileSplitProvider = opDesc.getTreeIndexFileSplitProvider();
                    IndexRegistry<ITreeIndex> treeIndexRegistry = opDesc.getTreeIndexRegistryProvider()
                            .getRegistry(ctx);
                    FileReference btreeFile = btreeFileSplitProvider.getFileSplits()[partition].getLocalFile();
                    boolean btreeFileIsMapped = fileMapProvider.isMapped(btreeFile);
                    if (!btreeFileIsMapped) {
                        throw new HyracksDataException(
                                "Trying to create inverted index, but associated BTree file has not been mapped");
                    }
                    int btreeFileId = fileMapProvider.lookupFileId(f);
                    BTree btree = (BTree) treeIndexRegistry.get(btreeFileId);

                    invIndex = new InvertedIndex(bufferCache, btree, opDesc.getInvIndexTypeTraits(), cmp);
                    invIndex.open(invIndexFileId);
                    invIndexRegistry.register(invIndexFileId, invIndex);
                }
            } finally {
                invIndexRegistry.unlock();
            }
        }
    }

    public void deinit() throws HyracksDataException {
        if (invIndexFileId != -1) {
            IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
            bufferCache.closeFile(invIndexFileId);
        }
    }

    public InvertedIndex getInvIndex() {
        return invIndex;
    }

    public ITreeIndexOperatorDescriptorHelper getOperatorDescriptor() {
        return opDesc;
    }

    public int getInvIndexFileId() {
        return invIndexFileId;
    }
}