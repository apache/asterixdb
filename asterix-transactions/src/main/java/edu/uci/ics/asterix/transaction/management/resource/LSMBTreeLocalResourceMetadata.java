package edu.uci.ics.asterix.transaction.management.resource;

import java.io.File;

import edu.uci.ics.asterix.transaction.management.service.recovery.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.IInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IInMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.file.TransientFileMapManager;

public class LSMBTreeLocalResourceMetadata implements ILocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] typeTraits;
    private final IBinaryComparatorFactory[] cmpFactories;
    private final int[] bloomFilterKeyFields;
    private final int memPageSize;
    private final int memNumPages;

    public LSMBTreeLocalResourceMetadata(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, boolean isPrimary, int memPageSize, int memNumPages) {
        this.typeTraits = typeTraits;
        this.cmpFactories = cmpFactories;
        this.bloomFilterKeyFields = bloomFilterKeyFields;
        this.memPageSize = memPageSize;
        this.memNumPages = memNumPages;
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider, String filePath,
            int partition) {
        FileReference file = new FileReference(new File(filePath));
        IInMemoryBufferCache memBufferCache = new InMemoryBufferCache(new HeapBufferAllocator(), memPageSize,
                memNumPages, new TransientFileMapManager());
        ITreeIndexMetaDataFrameFactory metaDataFrameFactory = new LIFOMetaDataFrameFactory();
        IInMemoryFreePageManager memFreePageManager = new InMemoryFreePageManager(memNumPages, metaDataFrameFactory);
        LSMBTree lsmBTree = LSMBTreeUtils.createLSMTree(memBufferCache, memFreePageManager,
                runtimeContextProvider.getIOManager(), file, runtimeContextProvider.getBufferCache(),
                runtimeContextProvider.getFileMapManager(), typeTraits, cmpFactories, bloomFilterKeyFields,
                runtimeContextProvider.getLSMMergePolicy(),
                runtimeContextProvider.getLSMBTreeOperationTrackerFactory(),
                runtimeContextProvider.getLSMIOScheduler(),
                runtimeContextProvider.getLSMBTreeIOOperationCallbackProvider(), partition);
        return lsmBTree;
    }

}
