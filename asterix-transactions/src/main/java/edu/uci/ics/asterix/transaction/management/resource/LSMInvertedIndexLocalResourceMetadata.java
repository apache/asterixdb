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
package edu.uci.ics.asterix.transaction.management.resource;

import edu.uci.ics.asterix.common.context.BaseOperationTracker;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMInvertedIndexIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;

public class LSMInvertedIndexLocalResourceMetadata extends AbstractLSMLocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] invListTypeTraits;
    private final IBinaryComparatorFactory[] invListCmpFactories;
    private final ITypeTraits[] tokenTypeTraits;
    private final IBinaryComparatorFactory[] tokenCmpFactories;
    private final IBinaryTokenizerFactory tokenizerFactory;
    private final boolean isPartitioned;
    private final FileSplit[] fileSplits;

    public LSMInvertedIndexLocalResourceMetadata(ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            boolean isPartitioned, FileSplit[] fileSplits, int datasetID) {
        super(datasetID);
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.tokenizerFactory = tokenizerFactory;
        this.isPartitioned = isPartitioned;
        this.fileSplits = fileSplits;
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider, String filePath,
            int partition) throws HyracksDataException {
        IVirtualBufferCache virtualBufferCache = runtimeContextProvider.getVirtualBufferCache(datasetID);
        try {
            if (isPartitioned) {
                return InvertedIndexUtils.createPartitionedLSMInvertedIndex(virtualBufferCache, runtimeContextProvider
                        .getFileMapManager(), invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                        tokenCmpFactories, tokenizerFactory, runtimeContextProvider.getBufferCache(),
                        runtimeContextProvider.getIOManager(), filePath, runtimeContextProvider
                                .getBloomFilterFalsePositiveRate(), runtimeContextProvider.getLSMMergePolicy(),
                        new BaseOperationTracker(LSMInvertedIndexIOOperationCallbackFactory.INSTANCE),
                        runtimeContextProvider.getLSMIOScheduler(), runtimeContextProvider
                                .getLSMInvertedIndexIOOperationCallbackProvider(), fileSplits[partition]
                                .getIODeviceId());
            } else {
                return InvertedIndexUtils.createLSMInvertedIndex(virtualBufferCache, runtimeContextProvider
                        .getFileMapManager(), invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                        tokenCmpFactories, tokenizerFactory, runtimeContextProvider.getBufferCache(),
                        runtimeContextProvider.getIOManager(), filePath, runtimeContextProvider
                                .getBloomFilterFalsePositiveRate(), runtimeContextProvider.getLSMMergePolicy(),
                        new BaseOperationTracker(LSMInvertedIndexIOOperationCallbackFactory.INSTANCE),
                        runtimeContextProvider.getLSMIOScheduler(), runtimeContextProvider
                                .getLSMInvertedIndexIOOperationCallbackProvider(), fileSplits[partition]
                                .getIODeviceId());
            }
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
    }
}
