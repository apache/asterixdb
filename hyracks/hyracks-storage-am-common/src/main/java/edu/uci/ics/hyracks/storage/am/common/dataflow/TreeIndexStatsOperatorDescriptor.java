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
package edu.uci.ics.hyracks.storage.am.common.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.file.NoOpLocalResourceFactoryProvider;

public class TreeIndexStatsOperatorDescriptor extends AbstractTreeIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final RecordDescriptor recDesc = new RecordDescriptor(
            new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });

    public TreeIndexStatsOperatorDescriptor(IOperatorDescriptorRegistry spec, IStorageManagerInterface storageManager,
            IIndexLifecycleManagerProvider lifecycleManagerProvider, IFileSplitProvider fileSplitProvider,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] comparatorFactories, int[] bloomFilterKeyFields,
            IIndexDataflowHelperFactory dataflowHelperFactory, ISearchOperationCallbackFactory searchOpCallbackProvider) {
        super(spec, 0, 1, recDesc, storageManager, lifecycleManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, bloomFilterKeyFields, dataflowHelperFactory, null, false,
                NoOpLocalResourceFactoryProvider.INSTANCE, searchOpCallbackProvider,
                NoOpOperationCallbackFactory.INSTANCE);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new TreeIndexStatsOperatorNodePushable(this, ctx, partition);
    }
}