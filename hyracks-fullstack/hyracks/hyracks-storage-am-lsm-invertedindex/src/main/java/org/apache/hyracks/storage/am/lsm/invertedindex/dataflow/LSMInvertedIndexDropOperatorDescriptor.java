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
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.IPageManagerFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorNodePushable;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.file.NoOpLocalResourceFactoryProvider;

public class LSMInvertedIndexDropOperatorDescriptor extends AbstractLSMInvertedIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    public LSMInvertedIndexDropOperatorDescriptor(IOperatorDescriptorRegistry spec, IStorageManager storageManager,
            IIndexLifecycleManagerProvider lifecycleManagerProvider, IFileSplitProvider fileSplitProvider,
            IIndexDataflowHelperFactory dataflowHelperFactory, IPageManagerFactory pageManagerFactory) {
        super(spec, 0, 0, null, storageManager, fileSplitProvider, lifecycleManagerProvider, new ITypeTraits[] {
                        null },
                new IBinaryComparatorFactory[] { null }, new ITypeTraits[] {
                        null }, new IBinaryComparatorFactory[] { null }, null,
                dataflowHelperFactory, null, false, false, null, NoOpLocalResourceFactoryProvider.INSTANCE,
                NoOpOperationCallbackFactory.INSTANCE, NoOpOperationCallbackFactory.INSTANCE, pageManagerFactory);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new IndexDropOperatorNodePushable(this, ctx, partition);
    }
}
