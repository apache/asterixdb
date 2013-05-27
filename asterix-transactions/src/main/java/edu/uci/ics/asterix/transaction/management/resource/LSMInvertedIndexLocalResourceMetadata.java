package edu.uci.ics.asterix.transaction.management.resource;

import edu.uci.ics.asterix.transaction.management.service.recovery.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.file.TransientFileMapManager;

public class LSMInvertedIndexLocalResourceMetadata implements ILocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] invListTypeTraits;
    private final IBinaryComparatorFactory[] invListCmpFactories;
    private final ITypeTraits[] tokenTypeTraits;
    private final IBinaryComparatorFactory[] tokenCmpFactories;
    private final IBinaryTokenizerFactory tokenizerFactory;
    private final int memPageSize;
    private final int memNumPages;
    private final boolean isPartitioned;

    public LSMInvertedIndexLocalResourceMetadata(ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory, int memPageSize,
            int memNumPages, boolean isPartitioned) {
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.tokenizerFactory = tokenizerFactory;
        this.memPageSize = memPageSize;
        this.memNumPages = memNumPages;
        this.isPartitioned = isPartitioned;
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider, String filePath,
            int partition) throws HyracksDataException {
        IVirtualBufferCache virtualBufferCache = new VirtualBufferCache(new HeapBufferAllocator(),
                new TransientFileMapManager(), memPageSize, memNumPages);
        try {
            if (isPartitioned) {
                return InvertedIndexUtils.createPartitionedLSMInvertedIndex(virtualBufferCache,
                        runtimeContextProvider.getFileMapManager(), invListTypeTraits, invListCmpFactories,
                        tokenTypeTraits, tokenCmpFactories, tokenizerFactory, runtimeContextProvider.getBufferCache(),
                        runtimeContextProvider.getIOManager(), filePath,
                        runtimeContextProvider.getBloomFilterFalsePositiveRate(),
                        runtimeContextProvider.getLSMMergePolicy(),
                        runtimeContextProvider.getLSMInvertedIndexOperationTrackerFactory(),
                        runtimeContextProvider.getLSMIOScheduler(),
                        runtimeContextProvider.getLSMInvertedIndexIOOperationCallbackProvider(), partition);
            } else {
                return InvertedIndexUtils.createLSMInvertedIndex(virtualBufferCache,
                        runtimeContextProvider.getFileMapManager(), invListTypeTraits, invListCmpFactories,
                        tokenTypeTraits, tokenCmpFactories, tokenizerFactory, runtimeContextProvider.getBufferCache(),
                        runtimeContextProvider.getIOManager(), filePath,
                        runtimeContextProvider.getBloomFilterFalsePositiveRate(),
                        runtimeContextProvider.getLSMMergePolicy(),
                        runtimeContextProvider.getLSMInvertedIndexOperationTrackerFactory(),
                        runtimeContextProvider.getLSMIOScheduler(),
                        runtimeContextProvider.getLSMInvertedIndexIOOperationCallbackProvider(), partition);
            }
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
    }
}
