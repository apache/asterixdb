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
package edu.uci.ics.hyracks.storage.am.common.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.impls.TreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class TreeIndexOpHelper {

    protected ITreeIndexFrame interiorFrame;
    protected ITreeIndexFrame leafFrame;
    protected MultiComparator cmp;

    protected ITreeIndex treeIndex;
    protected int indexFileId = -1;
    protected int partition;

    protected ITreeIndexOperatorDescriptorHelper opDesc;
    protected IHyracksTaskContext ctx;

    protected IndexHelperOpenMode mode;

    public TreeIndexOpHelper(ITreeIndexOperatorDescriptorHelper opDesc, final IHyracksTaskContext ctx, int partition,
            IndexHelperOpenMode mode) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.mode = mode;
        this.partition = partition;
    }

    public void init() throws HyracksDataException {
        IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
        IFileMapProvider fileMapProvider = opDesc.getStorageManager().getFileMapProvider(ctx);
        IFileSplitProvider fileSplitProvider = opDesc.getTreeIndexFileSplitProvider();

        FileReference f = fileSplitProvider.getFileSplits()[partition].getLocalFile();
        boolean fileIsMapped = fileMapProvider.isMapped(f);

        switch (mode) {

            case OPEN: {
                if (!fileIsMapped) {
                    throw new HyracksDataException("Trying to open tree index from unmapped file " + f.toString());
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

        // only set indexFileId member when openFile() succeeds,
        // otherwise deinit() will try to close the file that failed to open
        indexFileId = fileId;

        interiorFrame = opDesc.getTreeIndexInteriorFactory().createFrame();
        leafFrame = opDesc.getTreeIndexLeafFactory().createFrame();

        IndexRegistry<ITreeIndex> treeIndexRegistry = opDesc.getTreeIndexRegistryProvider().getRegistry(ctx);
        treeIndex = treeIndexRegistry.get(indexFileId);
        if (treeIndex == null) {

            // create new tree and register it
            treeIndexRegistry.lock();
            try {
                // check if tree has already been registered by another thread
                treeIndex = treeIndexRegistry.get(indexFileId);
                if (treeIndex == null) {
                    // this thread should create and register the tree

                    IBinaryComparator[] comparators = new IBinaryComparator[opDesc.getTreeIndexComparatorFactories().length];
                    for (int i = 0; i < opDesc.getTreeIndexComparatorFactories().length; i++) {
                        comparators[i] = opDesc.getTreeIndexComparatorFactories()[i].createBinaryComparator();
                    }

                    cmp = new MultiComparator(opDesc.getTreeIndexTypeTraits(), comparators);

                    treeIndex = createTreeIndex();
                    if (mode == IndexHelperOpenMode.CREATE) {
                        ITreeIndexMetaDataFrame metaFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory()
                                .createFrame();
                        try {
                            treeIndex.create(indexFileId, leafFrame, metaFrame);
                        } catch (Exception e) {
                            throw new HyracksDataException(e);
                        }
                    }
                    treeIndex.open(indexFileId);
                    treeIndexRegistry.register(indexFileId, treeIndex);
                }
            } finally {
                treeIndexRegistry.unlock();
            }
        }
    }

    // MUST be overridden
    public ITreeIndex createTreeIndex() throws HyracksDataException {
        throw new HyracksDataException("createTreeIndex Operation not implemented.");
    }

    public ITreeIndexCursor createDiskOrderScanCursor(ITreeIndexFrame leafFrame) throws HyracksDataException {
        return new TreeDiskOrderScanCursor(leafFrame);
    }

    public void deinit() throws HyracksDataException {
        if (indexFileId != -1) {
            IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
            bufferCache.closeFile(indexFileId);
        }
    }

    public ITreeIndex getTreeIndex() {
        return treeIndex;
    }

    public IHyracksTaskContext getHyracksTaskContext() {
        return ctx;
    }

    public ITreeIndexOperatorDescriptorHelper getOperatorDescriptor() {
        return opDesc;
    }

    public ITreeIndexFrame getLeafFrame() {
        return leafFrame;
    }

    public ITreeIndexFrame getInteriorFrame() {
        return interiorFrame;
    }

    public int getIndexFileId() {
        return indexFileId;
    }
}