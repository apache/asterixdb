package edu.uci.ics.asterix.transaction.management.resource;

import java.io.File;

import edu.uci.ics.asterix.transaction.management.service.recovery.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.IInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IInMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.DualIndexInMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.DualIndexInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;

public class LSMRTreeLocalResourceMetadata implements ILocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] typeTraits;
    private final IBinaryComparatorFactory[] rtreeCmpFactories;
    private final IBinaryComparatorFactory[] btreeCmpFactories;
    private final IPrimitiveValueProviderFactory[] valueProviderFactories;
    private final RTreePolicyType rtreePolicyType;
    private final ILinearizeComparatorFactory linearizeCmpFactory;
    private final int memPageSize;
    private final int memNumPages;

    public LSMRTreeLocalResourceMetadata(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, ILinearizeComparatorFactory linearizeCmpFactory, int memPageSize,
            int memNumPages) {
        this.typeTraits = typeTraits;
        this.rtreeCmpFactories = rtreeCmpFactories;
        this.btreeCmpFactories = btreeCmpFactories;
        this.valueProviderFactories = valueProviderFactories;
        this.rtreePolicyType = rtreePolicyType;
        this.linearizeCmpFactory = linearizeCmpFactory;
        this.memPageSize = memPageSize;
        this.memNumPages = memNumPages;
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider, String filePath,
            int partition) throws HyracksDataException {
        FileReference file = new FileReference(new File(filePath));
        ITreeIndexMetaDataFrameFactory metaDataFrameFactory = new LIFOMetaDataFrameFactory();
        IInMemoryBufferCache memBufferCache = new DualIndexInMemoryBufferCache(new HeapBufferAllocator(), memPageSize,
                memNumPages);
        IInMemoryFreePageManager memFreePageManager = new DualIndexInMemoryFreePageManager(memNumPages,
                metaDataFrameFactory);

        try {
            return LSMRTreeUtils.createLSMTree(memBufferCache, memFreePageManager,
                    runtimeContextProvider.getIOManager(), file, runtimeContextProvider.getBufferCache(),
                    runtimeContextProvider.getFileMapManager(), typeTraits, rtreeCmpFactories, btreeCmpFactories,
                    valueProviderFactories, rtreePolicyType, runtimeContextProvider.getLSMMergePolicy(),
                    runtimeContextProvider.getLSMRTreeOperationTrackerFactory(),
                    runtimeContextProvider.getLSMIOScheduler(),
                    runtimeContextProvider.getLSMRTreeIOOperationCallbackProvider(), linearizeCmpFactory, partition);
        } catch (TreeIndexException e) {
            throw new HyracksDataException(e);
        }
    }
}
