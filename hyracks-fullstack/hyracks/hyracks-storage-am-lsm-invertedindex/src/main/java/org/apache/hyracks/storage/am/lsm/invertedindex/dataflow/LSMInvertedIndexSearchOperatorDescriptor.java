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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifierFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;

public class LSMInvertedIndexSearchOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final int queryField;
    private final IInvertedIndexSearchModifierFactory searchModifierFactory;
    private final int[] minFilterFieldIndexes;
    private final int[] maxFilterFieldIndexes;
    private final boolean appendIndexFilter;
    private final boolean isFullTextSearchQuery;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final IBinaryTokenizerFactory queryTokenizerFactory;
    private final boolean retainInput;
    private final boolean retainMissing;
    private final IMissingWriterFactory missingWriterFactory;
    private final ISearchOperationCallbackFactory searchCallbackFactory;
    private final int numOfFields;
    // the maximum number of frames that this inverted-index-search can use
    private final int frameLimit;

    public LSMInvertedIndexSearchOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int queryField, IIndexDataflowHelperFactory indexHelperFactory,
            IBinaryTokenizerFactory queryTokenizerFactory, IInvertedIndexSearchModifierFactory searchModifierFactory,
            boolean retainInput, boolean retainMissing, IMissingWriterFactory missingWriterFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, int[] minFilterFieldIndexes,
            int[] maxFilterFieldIndexes, boolean isFullTextSearchQuery, int numOfFields, boolean appendIndexFilter,
            int frameLimit) {
        super(spec, 1, 1);
        this.indexHelperFactory = indexHelperFactory;
        this.queryTokenizerFactory = queryTokenizerFactory;
        this.queryField = queryField;
        this.retainInput = retainInput;
        this.retainMissing = retainMissing;
        this.missingWriterFactory = missingWriterFactory;
        this.searchModifierFactory = searchModifierFactory;
        this.searchCallbackFactory = searchCallbackFactory;
        this.minFilterFieldIndexes = minFilterFieldIndexes;
        this.maxFilterFieldIndexes = maxFilterFieldIndexes;
        this.isFullTextSearchQuery = isFullTextSearchQuery;
        this.appendIndexFilter = appendIndexFilter;
        this.numOfFields = numOfFields;
        this.outRecDescs[0] = outRecDesc;
        this.frameLimit = frameLimit;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        IInvertedIndexSearchModifier searchModifier = searchModifierFactory.createSearchModifier();
        return new LSMInvertedIndexSearchOperatorNodePushable(ctx,
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), partition, minFilterFieldIndexes,
                maxFilterFieldIndexes, indexHelperFactory, retainInput, retainMissing, missingWriterFactory,
                searchCallbackFactory, searchModifier, queryTokenizerFactory, queryField, isFullTextSearchQuery,
                numOfFields, appendIndexFilter, frameLimit);
    }
}
