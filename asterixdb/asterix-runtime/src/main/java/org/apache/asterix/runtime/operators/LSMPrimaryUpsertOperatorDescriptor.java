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
package org.apache.asterix.runtime.operators;

import org.apache.asterix.common.dataflow.LSMTreeInsertDeleteOperatorDescriptor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallbackFactory;

public class LSMPrimaryUpsertOperatorDescriptor extends LSMTreeInsertDeleteOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final IFrameOperationCallbackFactory frameOpCallbackFactory;
    private final ARecordType recordType;
    private final int filterIndex;
    private ISearchOperationCallbackFactory searchOpCallbackFactory;
    private final int numPrimaryKeys;
    private final IMissingWriterFactory missingWriterFactory;
    private final boolean hasSecondaries;

    public LSMPrimaryUpsertOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] fieldPermutation, IIndexDataflowHelperFactory indexHelperFactory,
            IMissingWriterFactory missingWriterFactory,
            IModificationOperationCallbackFactory modificationOpCallbackFactory,
            ISearchOperationCallbackFactory searchOpCallbackFactory,
            IFrameOperationCallbackFactory frameOpCallbackFactory, int numPrimaryKeys, ARecordType recordType,
            int filterIndex, boolean hasSecondaries) {
        super(spec, outRecDesc, fieldPermutation, IndexOperation.UPSERT, indexHelperFactory, null, true,
                modificationOpCallbackFactory);
        this.frameOpCallbackFactory = frameOpCallbackFactory;
        this.searchOpCallbackFactory = searchOpCallbackFactory;
        this.numPrimaryKeys = numPrimaryKeys;
        this.missingWriterFactory = missingWriterFactory;
        this.recordType = recordType;
        this.filterIndex = filterIndex;
        this.hasSecondaries = hasSecondaries;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor intputRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new LSMPrimaryUpsertOperatorNodePushable(ctx, partition, indexHelperFactory, fieldPermutation,
                intputRecDesc, modCallbackFactory, searchOpCallbackFactory, numPrimaryKeys, recordType, filterIndex,
                frameOpCallbackFactory, missingWriterFactory, hasSecondaries);
    }
}
