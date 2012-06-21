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

package edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushPolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;

public class LSMBTreeDataflowHelper extends TreeIndexDataflowHelper {
    private static int DEFAULT_MEM_PAGE_SIZE = 32768;
    private static int DEFAULT_MEM_NUM_PAGES = 1000;

    private final int memPageSize;
    private final int memNumPages;

    private final ILSMFlushPolicy flushPolicy;
    private final ILSMMergePolicy mergePolicy;

    public LSMBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            ILSMFlushPolicy flushPolicy, ILSMMergePolicy mergePolicy) {
        this(opDesc, ctx, partition, DEFAULT_MEM_PAGE_SIZE, DEFAULT_MEM_NUM_PAGES, flushPolicy, mergePolicy);
    }

    public LSMBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            int memPageSize, int memNumPages, ILSMFlushPolicy flushPolicy, ILSMMergePolicy mergePolicy) {
        super(opDesc, ctx, partition);
        this.memPageSize = memPageSize;
        this.memNumPages = memNumPages;
        this.flushPolicy = flushPolicy;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public ITreeIndex createIndexInstance() throws HyracksDataException {
        ITreeIndexMetaDataFrameFactory metaDataFrameFactory = new LIFOMetaDataFrameFactory();
        InMemoryBufferCache memBufferCache = new InMemoryBufferCache(new HeapBufferAllocator(), memPageSize,
                memNumPages);
        IFileSplitProvider fileSplitProvider = opDesc.getFileSplitProvider();
        FileReference file = fileSplitProvider.getFileSplits()[partition].getLocalFile();
        if (file.getFile().exists() && !file.getFile().isDirectory()) {
            file.delete();
        }
        InMemoryFreePageManager memFreePageManager = new InMemoryFreePageManager(memNumPages, metaDataFrameFactory);
        return LSMBTreeUtils.createLSMTree(memBufferCache, memFreePageManager,
                ctx.getIOManager(), file.getFile().getPath(), opDesc.getStorageManager()
                        .getBufferCache(ctx), opDesc.getStorageManager().getFileMapProvider(ctx), treeOpDesc
                        .getTreeIndexTypeTraits(), treeOpDesc.getTreeIndexComparatorFactories(), flushPolicy, mergePolicy);
    }
}
