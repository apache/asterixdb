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

package org.apache.hyracks.storage.am.btree.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;

public class BTreeSearchOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    protected final int[] lowKeyFields;
    protected final int[] highKeyFields;
    protected final boolean lowKeyInclusive;
    protected final boolean highKeyInclusive;
    private final int[] minFilterFieldIndexes;
    private final int[] maxFilterFieldIndexes;
    protected final IIndexDataflowHelperFactory indexHelperFactory;
    protected final boolean retainInput;
    protected final boolean retainMissing;
    protected final IMissingWriterFactory missingWriterFactory;
    protected final ISearchOperationCallbackFactory searchCallbackFactory;
    protected final boolean appendIndexFilter;
    protected boolean appendOpCallbackProceedResult;
    protected byte[] searchCallbackProceedResultFalseValue;
    protected byte[] searchCallbackProceedResultTrueValue;
    protected final ITupleFilterFactory tupleFilterFactory;
    protected final long outputLimit;

    public BTreeSearchOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes, boolean appendIndexFilter) {
        this(spec, outRecDesc, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive, indexHelperFactory,
                retainInput, retainMissing, missingWriterFactory, searchCallbackFactory, minFilterFieldIndexes,
                maxFilterFieldIndexes, appendIndexFilter, null, -1, false, null, null);
    }

    public BTreeSearchOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes, boolean appendIndexFilter,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, boolean appendOpCallbackProceedResult,
            byte[] searchCallbackProceedResultFalseValue, byte[] searchCallbackProceedResultTrueValue) {
        super(spec, 1, 1);
        this.indexHelperFactory = indexHelperFactory;
        this.retainInput = retainInput;
        this.retainMissing = retainMissing;
        this.missingWriterFactory = missingWriterFactory;
        this.searchCallbackFactory = searchCallbackFactory;
        this.lowKeyFields = lowKeyFields;
        this.highKeyFields = highKeyFields;
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
        this.minFilterFieldIndexes = minFilterFieldIndexes;
        this.maxFilterFieldIndexes = maxFilterFieldIndexes;
        this.appendIndexFilter = appendIndexFilter;
        this.outRecDescs[0] = outRecDesc;
        this.tupleFilterFactory = tupleFilterFactory;
        this.outputLimit = outputLimit;
        this.appendOpCallbackProceedResult = appendOpCallbackProceedResult;
        this.searchCallbackProceedResultFalseValue = searchCallbackProceedResultFalseValue;
        this.searchCallbackProceedResultTrueValue = searchCallbackProceedResultTrueValue;
    }

    @Override
    public BTreeSearchOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new BTreeSearchOperatorNodePushable(ctx, partition,
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), lowKeyFields, highKeyFields,
                lowKeyInclusive, highKeyInclusive, minFilterFieldIndexes, maxFilterFieldIndexes, indexHelperFactory,
                retainInput, retainMissing, missingWriterFactory, searchCallbackFactory, appendIndexFilter,
                tupleFilterFactory, outputLimit, appendOpCallbackProceedResult, searchCallbackProceedResultFalseValue,
                searchCallbackProceedResultTrueValue);
    }

    @Override
    public String getDisplayName() {
        return "BTree Search";
    }

}
