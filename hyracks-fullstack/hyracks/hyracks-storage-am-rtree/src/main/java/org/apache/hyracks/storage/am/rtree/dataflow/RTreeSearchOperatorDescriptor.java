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

package org.apache.hyracks.storage.am.rtree.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.common.IStorageManagerInterface;
import org.apache.hyracks.storage.common.file.NoOpLocalResourceFactoryProvider;

public class RTreeSearchOperatorDescriptor extends AbstractTreeIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    protected int[] keyFields; // fields in input tuple to be used as keys
    protected final int[] minFilterFieldIndexes;
    protected final int[] maxFilterFieldIndexes;

    public RTreeSearchOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            IStorageManagerInterface storageManager, IIndexLifecycleManagerProvider lifecycleManagerProvider,
            IFileSplitProvider fileSplitProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, int[] keyFields,
            IIndexDataflowHelperFactory dataflowHelperFactory, boolean retainInput, boolean retainNull,
            IMissingWriterFactory nullWriterFactory, ISearchOperationCallbackFactory searchOpCallbackFactory,
            int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes) {

        super(spec, 1, 1, recDesc, storageManager, lifecycleManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, null, dataflowHelperFactory, null, retainInput, retainNull, nullWriterFactory,
                NoOpLocalResourceFactoryProvider.INSTANCE, searchOpCallbackFactory,
                NoOpOperationCallbackFactory.INSTANCE);
        this.keyFields = keyFields;
        this.minFilterFieldIndexes = minFilterFieldIndexes;
        this.maxFilterFieldIndexes = maxFilterFieldIndexes;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new RTreeSearchOperatorNodePushable(this, ctx, partition, recordDescProvider, keyFields,
                minFilterFieldIndexes, maxFilterFieldIndexes);
    }
}
