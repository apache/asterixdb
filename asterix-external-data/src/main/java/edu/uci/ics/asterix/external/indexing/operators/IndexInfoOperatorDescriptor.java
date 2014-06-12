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
package edu.uci.ics.asterix.external.indexing.operators;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilterFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;

/*
 * This is a hack used to optain multiple index instances in a single operator and it is not actually used as an operator
 */
public class IndexInfoOperatorDescriptor implements IIndexOperatorDescriptor{

    private static final long serialVersionUID = 1L;
    private final IFileSplitProvider fileSplitProvider;
    private final IStorageManagerInterface storageManager;
    private final IIndexLifecycleManagerProvider lifecycleManagerProvider;
    public IndexInfoOperatorDescriptor(IFileSplitProvider fileSplitProvider,IStorageManagerInterface storageManager,
            IIndexLifecycleManagerProvider lifecycleManagerProvider){
        this.fileSplitProvider = fileSplitProvider;
        this.lifecycleManagerProvider = lifecycleManagerProvider;
        this.storageManager = storageManager;
        
    }

    @Override
    public ActivityId getActivityId() {
        return null;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return null;
    }

    @Override
    public IFileSplitProvider getFileSplitProvider() {
        return fileSplitProvider;
    }

    @Override
    public IStorageManagerInterface getStorageManager() {
        return storageManager;
    }

    @Override
    public IIndexLifecycleManagerProvider getLifecycleManagerProvider() {
        return lifecycleManagerProvider;
    }

    @Override
    public RecordDescriptor getRecordDescriptor() {
        return null;
    }

    @Override
    public IIndexDataflowHelperFactory getIndexDataflowHelperFactory() {
        return null;
    }

    @Override
    public boolean getRetainInput() {
        return false;
    }

    @Override
    public ISearchOperationCallbackFactory getSearchOpCallbackFactory() {
        return null;
    }

    @Override
    public IModificationOperationCallbackFactory getModificationOpCallbackFactory() {
        return null;
    }

    @Override
    public ITupleFilterFactory getTupleFilterFactory() {
        return null;
    }

    @Override
    public ILocalResourceFactoryProvider getLocalResourceFactoryProvider() {
        return null;
    }

    @Override
    public boolean getRetainNull() {
        return false;
    }

    @Override
    public INullWriterFactory getNullWriterFactory() {
        return null;
    }

}
