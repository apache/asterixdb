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

package org.apache.hyracks.storage.am.common.dataflow;

import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.IPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;

public abstract class AbstractIndexOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor implements
        IIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    protected final IFileSplitProvider fileSplitProvider;
    protected final IStorageManager storageManager;
    protected final IIndexLifecycleManagerProvider lifecycleManagerProvider;
    protected final IIndexDataflowHelperFactory dataflowHelperFactory;
    protected final ITupleFilterFactory tupleFilterFactory;
    protected final IPageManagerFactory pageManagerFactory;
    protected final boolean retainInput;
    protected final boolean retainNull;
    protected final IMissingWriterFactory nullWriterFactory;
    protected final ISearchOperationCallbackFactory searchOpCallbackFactory;
    protected final IModificationOperationCallbackFactory modificationOpCallbackFactory;
    protected final ILocalResourceFactoryProvider localResourceFactoryProvider;

    public AbstractIndexOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity,
            RecordDescriptor recDesc, IStorageManager storageManager,
            IIndexLifecycleManagerProvider lifecycleManagerProvider, IFileSplitProvider fileSplitProvider,
            IIndexDataflowHelperFactory dataflowHelperFactory, ITupleFilterFactory tupleFilterFactory,
            boolean retainInput, boolean retainNull, IMissingWriterFactory nullWriterFactory,
            ILocalResourceFactoryProvider localResourceFactoryProvider,
            ISearchOperationCallbackFactory searchOpCallbackFactory,
            IModificationOperationCallbackFactory modificationOpCallbackFactory,
            IPageManagerFactory pageManagerFactory) {
        super(spec, inputArity, outputArity);
        this.fileSplitProvider = fileSplitProvider;
        this.storageManager = storageManager;
        this.lifecycleManagerProvider = lifecycleManagerProvider;
        this.dataflowHelperFactory = dataflowHelperFactory;
        this.retainInput = retainInput;
        this.retainNull = retainNull;
        this.nullWriterFactory = nullWriterFactory;
        this.tupleFilterFactory = tupleFilterFactory;
        this.localResourceFactoryProvider = localResourceFactoryProvider;
        this.searchOpCallbackFactory = searchOpCallbackFactory;
        this.modificationOpCallbackFactory = modificationOpCallbackFactory;
        this.pageManagerFactory = pageManagerFactory;
        if (outputArity > 0) {
            recordDescriptors[0] = recDesc;
        }
    }

    @Override
    public IFileSplitProvider getFileSplitProvider() {
        return fileSplitProvider;
    }

    @Override
    public IStorageManager getStorageManager() {
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
    public boolean getRetainMissing() {
        return retainNull;
    }

    @Override
    public IMissingWriterFactory getMissingWriterFactory() {
        return nullWriterFactory;
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

    @Override
    public IPageManagerFactory getPageManagerFactory() {
        return pageManagerFactory;
    }
}
