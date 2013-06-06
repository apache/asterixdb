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
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.file.NoOpLocalResourceFactoryProvider;

public class IndexDropOperatorDescriptor extends AbstractTreeIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    public IndexDropOperatorDescriptor(IOperatorDescriptorRegistry spec, IStorageManagerInterface storageManager,
            IIndexLifecycleManagerProvider lifecycleManagerProvider, IFileSplitProvider fileSplitProvider,
            IIndexDataflowHelperFactory dataflowHelperFactory) {
        // TODO: providing the type traits below is a hack to allow:
        // 1) Type traits not to be specified when creating the drop operator
        // 2) The LSMRTreeDataflowHelper to get acceptable type traits
        // This should eventually not be *hacked*, but I don't know the proper fix yet. -zheilbron
        super(spec, 0, 0, null, storageManager, lifecycleManagerProvider, fileSplitProvider, new ITypeTraits[] {
                IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS }, new IBinaryComparatorFactory[] { null }, null,
                dataflowHelperFactory, null, false, NoOpLocalResourceFactoryProvider.INSTANCE,
                NoOpOperationCallbackFactory.INSTANCE, NoOpOperationCallbackFactory.INSTANCE);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new IndexDropOperatorNodePushable(this, ctx, partition);
    }
}
