package edu.uci.ics.asterix.transaction.management.resource;

import java.io.File;

import edu.uci.ics.asterix.common.context.BaseOperationTracker;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;

public class LSMBTreeLocalResourceMetadata extends AbstractLSMLocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] typeTraits;
    private final IBinaryComparatorFactory[] cmpFactories;
    private final int[] bloomFilterKeyFields;
    private final boolean isPrimary;
    private FileSplit[] fileSplits;
    private int ioDeviceID;

    public LSMBTreeLocalResourceMetadata(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, boolean isPrimary, FileSplit[] fileSplits, int datasetID) {
        super(datasetID);
        this.typeTraits = typeTraits;
        this.cmpFactories = cmpFactories;
        this.bloomFilterKeyFields = bloomFilterKeyFields;
        this.isPrimary = isPrimary;
        this.fileSplits = fileSplits;
    }

    public LSMBTreeLocalResourceMetadata(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, boolean isPrimary, int ioDeviceID, int datasetID) {
        super(datasetID);
        this.typeTraits = typeTraits;
        this.cmpFactories = cmpFactories;
        this.bloomFilterKeyFields = bloomFilterKeyFields;
        this.isPrimary = isPrimary;
        this.ioDeviceID = ioDeviceID;
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider, String filePath,
            int partition) {
        FileReference file = new FileReference(new File(filePath));
        IVirtualBufferCache virtualBufferCache = runtimeContextProvider.getVirtualBufferCache(datasetID);
        LSMBTree lsmBTree = LSMBTreeUtils.createLSMTree(virtualBufferCache, runtimeContextProvider.getIOManager(),
                file, runtimeContextProvider.getBufferCache(), runtimeContextProvider.getFileMapManager(), typeTraits,
                cmpFactories, bloomFilterKeyFields, runtimeContextProvider.getBloomFilterFalsePositiveRate(),
                runtimeContextProvider.getLSMMergePolicy(),
                isPrimary ? runtimeContextProvider.getLSMBTreeOperationTracker(datasetID) : new BaseOperationTracker(
                        LSMBTreeIOOperationCallbackFactory.INSTANCE), runtimeContextProvider.getLSMIOScheduler(),
                runtimeContextProvider.getLSMBTreeIOOperationCallbackProvider(), fileSplits == null ? ioDeviceID
                        : fileSplits[partition].getIODeviceId());
        return lsmBTree;
    }

}
