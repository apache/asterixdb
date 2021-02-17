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
package org.apache.hyracks.storage.am.lsm.invertedindex.dataflow;

import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LsmResourceFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfigEvaluatorFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.IStorageManager;

public class LSMInvertedIndexLocalResourceFactory extends LsmResourceFactory {

    private static final long serialVersionUID = 1L;
    private final ITypeTraits[] tokenTypeTraits;
    private final IBinaryComparatorFactory[] tokenCmpFactories;
    private final IBinaryTokenizerFactory tokenizerFactory;
    private final boolean isPartitioned;
    private final int[] invertedIndexFields;
    private final int[] filterFieldsForNonBulkLoadOps;
    private final int[] invertedIndexFieldsForNonBulkLoadOps;
    private final double bloomFilterFalsePositiveRate;
    private final IFullTextConfigEvaluatorFactory fullTextConfigEvaluatorFactory;

    public LSMInvertedIndexLocalResourceFactory(IStorageManager storageManager, ITypeTraits[] typeTraits,
            // Inherited fields
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerFactory, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable,
            // New fields
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, IFullTextConfigEvaluatorFactory fullTextConfigEvaluatorFactory,
            boolean isPartitioned, int[] invertedIndexFields, int[] filterFieldsForNonBulkLoadOps,
            int[] invertedIndexFieldsForNonBulkLoadOps, double bloomFilterFalsePositiveRate) {
        super(storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerFactory, ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, durable);
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        // ToDo: totally replace tokenizerFactory with full-text config
        this.tokenizerFactory = tokenizerFactory;
        this.fullTextConfigEvaluatorFactory = fullTextConfigEvaluatorFactory;
        this.isPartitioned = isPartitioned;
        this.invertedIndexFields = invertedIndexFields;
        this.filterFieldsForNonBulkLoadOps = filterFieldsForNonBulkLoadOps;
        this.invertedIndexFieldsForNonBulkLoadOps = invertedIndexFieldsForNonBulkLoadOps;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
    }

    @Override
    public IResource createResource(FileReference fileRef) {
        return new LSMInvertedIndexLocalResource(fileRef.getRelativePath(), storageManager, typeTraits, cmpFactories,
                filterTypeTraits, filterCmpFactories, filterFields, opTrackerProvider, ioOpCallbackFactory,
                pageWriteCallbackFactory, metadataPageManagerFactory, vbcProvider, ioSchedulerProvider,
                mergePolicyFactory, mergePolicyProperties, durable, tokenTypeTraits, tokenCmpFactories,
                tokenizerFactory, fullTextConfigEvaluatorFactory, isPartitioned, invertedIndexFields,
                filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, bloomFilterFalsePositiveRate);
    }

}
