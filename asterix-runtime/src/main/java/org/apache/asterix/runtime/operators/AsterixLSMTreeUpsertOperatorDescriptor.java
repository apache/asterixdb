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

import org.apache.asterix.common.dataflow.AsterixLSMTreeInsertDeleteOperatorDescriptor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.common.IStorageManagerInterface;

public class AsterixLSMTreeUpsertOperatorDescriptor extends AsterixLSMTreeInsertDeleteOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final int[] prevValuePermutation;
    private ARecordType type;
    private int filterIndex = -1;

    public AsterixLSMTreeUpsertOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            IStorageManagerInterface storageManager, IIndexLifecycleManagerProvider lifecycleManagerProvider,
            IFileSplitProvider fileSplitProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, int[] bloomFilterKeyFields, int[] fieldPermutation,
            IIndexDataflowHelperFactory dataflowHelperFactory, ITupleFilterFactory tupleFilterFactory,
            boolean isPrimary, String indexName, INullWriterFactory nullWriterFactory,
            IModificationOperationCallbackFactory modificationOpCallbackProvider,
            ISearchOperationCallbackFactory searchOpCallbackProvider, int[] prevValuePermutation) {
        super(spec, recDesc, storageManager, lifecycleManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, bloomFilterKeyFields, fieldPermutation, IndexOperation.UPSERT,
                dataflowHelperFactory, tupleFilterFactory, isPrimary, indexName, nullWriterFactory,
                modificationOpCallbackProvider, searchOpCallbackProvider);
        this.prevValuePermutation = prevValuePermutation;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return isPrimary()
                ? new AsterixLSMPrimaryUpsertOperatorNodePushable(this, ctx, partition, fieldPermutation,
                        recordDescProvider, comparatorFactories.length, type, filterIndex)
                : new AsterixLSMSecondaryUpsertOperatorNodePushable(this, ctx, partition, fieldPermutation,
                        recordDescProvider, prevValuePermutation);
    }

    public void setType(ARecordType type) {
        this.type = type;
    }

    public void setFilterIndex(int filterIndex) {
        this.filterIndex = filterIndex;
    }
}
