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

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilterFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;

public abstract class AbstractIndexOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor implements
        IIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    protected final IFileSplitProvider fileSplitProvider;
    protected final IStorageManagerInterface storageManager;
    protected final IIndexLifecycleManagerProvider lifecycleManagerProvider;
    protected final IIndexDataflowHelperFactory dataflowHelperFactory;
    protected final ITupleFilterFactory tupleFilterFactory;
    protected final boolean retainInput;
    protected final ISearchOperationCallbackFactory searchOpCallbackFactory;
    protected final IModificationOperationCallbackFactory modificationOpCallbackFactory;
    protected final ILocalResourceFactoryProvider localResourceFactoryProvider;

    public AbstractIndexOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity,
            RecordDescriptor recDesc, IStorageManagerInterface storageManager,
            IIndexLifecycleManagerProvider lifecycleManagerProvider, IFileSplitProvider fileSplitProvider,
            IIndexDataflowHelperFactory dataflowHelperFactory, ITupleFilterFactory tupleFilterFactory,
            boolean retainInput, ILocalResourceFactoryProvider localResourceFactoryProvider,
            ISearchOperationCallbackFactory searchOpCallbackFactory,
            IModificationOperationCallbackFactory modificationOpCallbackFactory) {
        super(spec, inputArity, outputArity);
        this.fileSplitProvider = fileSplitProvider;
        this.storageManager = storageManager;
        this.lifecycleManagerProvider = lifecycleManagerProvider;
        this.dataflowHelperFactory = dataflowHelperFactory;
        this.retainInput = retainInput;
        this.tupleFilterFactory = tupleFilterFactory;
        this.localResourceFactoryProvider = localResourceFactoryProvider;
        this.searchOpCallbackFactory = searchOpCallbackFactory;
        this.modificationOpCallbackFactory = modificationOpCallbackFactory;
        if (outputArity > 0) {
            recordDescriptors[0] = recDesc;
        }
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
        return recordDescriptors[0];
    }

    @Override
    public IIndexDataflowHelperFactory getIndexDataflowHelperFactory() {
        return dataflowHelperFactory;
    }

    @Override
    public boolean getRetainInput() {
        return retainInput;
    }

    @Override
    public ISearchOperationCallbackFactory getSearchOpCallbackFactory() {
        return searchOpCallbackFactory;
    }
    
    @Override
    public IModificationOperationCallbackFactory getModificationOpCallbackFactory() {
        return modificationOpCallbackFactory;
    }

    @Override
    public ITupleFilterFactory getTupleFilterFactory() {
        return tupleFilterFactory;
    }
    
    @Override
    public ILocalResourceFactoryProvider getLocalResourceFactoryProvider() {
        return localResourceFactoryProvider;
    }
}
