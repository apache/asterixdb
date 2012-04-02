/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexIdProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class TreeIndexDiskOrderScanOperatorDescriptor extends AbstractTreeIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    public TreeIndexDiskOrderScanOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc,
            IStorageManagerInterface storageManager, IIndexRegistryProvider<IIndex> indexRegistryProvider,
            IFileSplitProvider fileSplitProvider, ITypeTraits[] typeTraits,
            IIndexDataflowHelperFactory dataflowHelperFactory, IOperationCallbackProvider opCallbackProvider,
            IIndexIdProvider indexIdProvider) {
        super(spec, 0, 1, recDesc, storageManager, indexRegistryProvider, fileSplitProvider, typeTraits, null,
                dataflowHelperFactory, opCallbackProvider, indexIdProvider);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new TreeIndexDiskOrderScanOperatorNodePushable(this, ctx, opCallbackProvider, indexIdProvider, partition);
    }
}