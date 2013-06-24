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

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.context.BaseOperationTracker;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMInvertedIndexIOOperationCallbackFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
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

    public LSMInvertedIndexLocalResourceMetadata(ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            boolean isPartitioned, int datasetID) {
        super(datasetID);
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.tokenizerFactory = tokenizerFactory;
        this.isPartitioned = isPartitioned;
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContext runtimeContext, String filePath, int partition)
            throws HyracksDataException {
        IVirtualBufferCache virtualBufferCache = runtimeContext.getVirtualBufferCache(datasetID);
        try {
            if (isPartitioned) {
                return InvertedIndexUtils.createPartitionedLSMInvertedIndex(virtualBufferCache,
                        runtimeContext.getFileMapManager(), invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                        tokenCmpFactories, tokenizerFactory, runtimeContext.getBufferCache(), filePath,
                        runtimeContext.getBloomFilterFalsePositiveRate(), runtimeContext.getLSMMergePolicy(),
                        new BaseOperationTracker(LSMInvertedIndexIOOperationCallbackFactory.INSTANCE),
                        runtimeContext.getLSMIOScheduler(),
                        runtimeContext.getLSMInvertedIndexIOOperationCallbackProvider());
            } else {
                return InvertedIndexUtils.createLSMInvertedIndex(virtualBufferCache,
                        runtimeContext.getFileMapManager(), invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                        tokenCmpFactories, tokenizerFactory, runtimeContext.getBufferCache(), filePath,
                        runtimeContext.getBloomFilterFalsePositiveRate(), runtimeContext.getLSMMergePolicy(),
                        new BaseOperationTracker(LSMInvertedIndexIOOperationCallbackFactory.INSTANCE),
                        runtimeContext.getLSMIOScheduler(),
                        runtimeContext.getLSMInvertedIndexIOOperationCallbackProvider());
            }
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
    }
}
