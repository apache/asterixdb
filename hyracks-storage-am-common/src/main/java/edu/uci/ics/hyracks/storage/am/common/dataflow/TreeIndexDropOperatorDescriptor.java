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

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class TreeIndexDropOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private IStorageManagerInterface storageManager;
    private IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider;
    private IFileSplitProvider fileSplitProvider;

    public TreeIndexDropOperatorDescriptor(JobSpecification spec, IStorageManagerInterface storageManager,
            IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider, IFileSplitProvider fileSplitProvider) {
        super(spec, 0, 0);
        this.storageManager = storageManager;
        this.treeIndexRegistryProvider = treeIndexRegistryProvider;
        this.fileSplitProvider = fileSplitProvider;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksStageletContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new TreeIndexDropOperatorNodePushable(ctx, storageManager, treeIndexRegistryProvider, fileSplitProvider,
                partition);
    }
}
