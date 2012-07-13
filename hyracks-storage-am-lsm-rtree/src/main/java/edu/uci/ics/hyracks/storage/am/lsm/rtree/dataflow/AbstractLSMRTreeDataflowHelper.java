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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTreeInMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTreeInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractLSMRTreeDataflowHelper extends TreeIndexDataflowHelper {
    private static int DEFAULT_MEM_PAGE_SIZE = 32768;
    private static int DEFAULT_MEM_NUM_PAGES = 1000;

    private final int memPageSize;
    private final int memNumPages;

    protected final IBinaryComparatorFactory[] btreeComparatorFactories;
    protected final IPrimitiveValueProviderFactory[] valueProviderFactories;
    protected final RTreePolicyType rtreePolicyType;
    protected final ILSMFlushController flushController;
    protected final ILSMMergePolicy mergePolicy;
    protected final ILSMOperationTracker opTracker;
    protected final ILSMIOScheduler ioScheduler;

    public AbstractLSMRTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            IBinaryComparatorFactory[] btreeComparatorFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            ILSMFlushController flushController, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOScheduler ioScheduler) {
        this(opDesc, ctx, partition, DEFAULT_MEM_PAGE_SIZE, DEFAULT_MEM_NUM_PAGES, btreeComparatorFactories,
                valueProviderFactories, rtreePolicyType, flushController, mergePolicy, opTracker, ioScheduler);
    }

    public AbstractLSMRTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            int memPageSize, int memNumPages, IBinaryComparatorFactory[] btreeComparatorFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            ILSMFlushController flushController, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOScheduler ioScheduler) {
        super(opDesc, ctx, partition);
        this.memPageSize = memPageSize;
        this.memNumPages = memNumPages;
        this.btreeComparatorFactories = btreeComparatorFactories;
        this.valueProviderFactories = valueProviderFactories;
        this.rtreePolicyType = rtreePolicyType;
        this.flushController = flushController;
        this.mergePolicy = mergePolicy;
        this.opTracker = opTracker;
        this.ioScheduler = ioScheduler;
    }

    @Override
    public ITreeIndex getIndexInstance() throws HyracksDataException {
        ITreeIndexMetaDataFrameFactory metaDataFrameFactory = new LIFOMetaDataFrameFactory();
        InMemoryBufferCache memBufferCache = new LSMRTreeInMemoryBufferCache(new HeapBufferAllocator(), memPageSize,
                memNumPages);
        InMemoryFreePageManager memFreePageManager = new LSMRTreeInMemoryFreePageManager(memNumPages,
                metaDataFrameFactory);

        return createLSMTree(memBufferCache, memFreePageManager, ctx.getIOManager(), file, opDesc.getStorageManager()
                .getBufferCache(ctx), opDesc.getStorageManager().getFileMapProvider(ctx),
                treeOpDesc.getTreeIndexTypeTraits(), treeOpDesc.getTreeIndexComparatorFactories(),
                btreeComparatorFactories, valueProviderFactories, rtreePolicyType);

    }

    protected abstract ITreeIndex createLSMTree(IBufferCache memBufferCache,
            InMemoryFreePageManager memFreePageManager, IIOManager ioManager, FileReference file,
            IBufferCache diskBufferCache, IFileMapProvider diskFileMapProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType)
            throws HyracksDataException;
}
