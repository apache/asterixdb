/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.trace.ITracer;

public class PartitionedLSMInvertedIndex extends LSMInvertedIndex {

    public PartitionedLSMInvertedIndex(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ILSMDiskComponentFactory componentFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, IBufferCache diskBufferCache, ILSMIndexFileManager fileManager,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory, int[] invertedIndexFields, int[] filterFields,
            int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps, boolean durable,
            ITracer tracer) throws HyracksDataException {
        super(ioManager, virtualBufferCaches, componentFactory, filterHelper, filterFrameFactory, filterManager,
                bloomFilterFalsePositiveRate, diskBufferCache, fileManager, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, tokenizerFactory, mergePolicy, opTracker, ioScheduler,
                ioOpCallbackFactory, pageWriteCallbackFactory, invertedIndexFields, filterFields,
                filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, durable, tracer);
    }

    @Override
    protected InMemoryInvertedIndex createInMemoryInvertedIndex(IVirtualBufferCache virtualBufferCache,
            VirtualFreePageManager virtualFreePageManager, int id) throws HyracksDataException {
        return InvertedIndexUtils.createPartitionedInMemoryBTreeInvertedindex(virtualBufferCache,
                virtualFreePageManager, invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories,
                tokenizerFactory, ioManager.resolveAbsolutePath(fileManager.getBaseDir() + "_virtual_vocab_" + id));
    }

}
