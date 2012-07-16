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

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilterFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public abstract class AbstractTreeIndexOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor implements
        ITreeIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    protected final IFileSplitProvider fileSplitProvider;

    protected final IBinaryComparatorFactory[] comparatorFactories;

    protected final IStorageManagerInterface storageManager;
    protected final IIndexLifecycleManagerProvider lifecycleManagerProvider;

    protected final ITypeTraits[] typeTraits;
    protected final IIndexDataflowHelperFactory dataflowHelperFactory;
    protected final ITupleFilterFactory tupleFilterFactory;

    protected final boolean retainInput;
    protected final IOperationCallbackProvider opCallbackProvider;

    public AbstractTreeIndexOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity,
            RecordDescriptor recDesc, IStorageManagerInterface storageManager,
            IIndexLifecycleManagerProvider lifecycleManagerProvider, IFileSplitProvider fileSplitProvider,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] comparatorFactories,
            IIndexDataflowHelperFactory dataflowHelperFactory, ITupleFilterFactory tupleFilterFactory,
            boolean retainInput, IOperationCallbackProvider opCallbackProvider) {
        super(spec, inputArity, outputArity);
        this.fileSplitProvider = fileSplitProvider;
        this.storageManager = storageManager;
        this.lifecycleManagerProvider = lifecycleManagerProvider;
        this.typeTraits = typeTraits;
        this.comparatorFactories = comparatorFactories;
        this.dataflowHelperFactory = dataflowHelperFactory;
        this.retainInput = retainInput;
        this.tupleFilterFactory = tupleFilterFactory;
        this.opCallbackProvider = opCallbackProvider;
        if (outputArity > 0) {
            recordDescriptors[0] = recDesc;
        }
    }

    @Override
    public IFileSplitProvider getFileSplitProvider() {
        return fileSplitProvider;
    }

    @Override
    public IBinaryComparatorFactory[] getTreeIndexComparatorFactories() {
        return comparatorFactories;
    }

    @Override
    public ITypeTraits[] getTreeIndexTypeTraits() {
        return typeTraits;
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
    public IOperationCallbackProvider getOpCallbackProvider() {
        return opCallbackProvider;
    }

    @Override
    public ITupleFilterFactory getTupleFilterFactory() {
        return tupleFilterFactory;
    }
}