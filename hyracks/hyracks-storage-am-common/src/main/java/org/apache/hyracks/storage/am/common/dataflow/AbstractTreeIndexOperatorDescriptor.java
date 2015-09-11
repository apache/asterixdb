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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.common.IStorageManagerInterface;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;

public abstract class AbstractTreeIndexOperatorDescriptor extends AbstractIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    protected final ITypeTraits[] typeTraits;
    protected final IBinaryComparatorFactory[] comparatorFactories;
    protected final int[] bloomFilterKeyFields;

    public AbstractTreeIndexOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity,
            RecordDescriptor recDesc, IStorageManagerInterface storageManager,
            IIndexLifecycleManagerProvider lifecycleManagerProvider, IFileSplitProvider fileSplitProvider,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] comparatorFactories, int[] bloomFilterKeyFields,
            IIndexDataflowHelperFactory dataflowHelperFactory, ITupleFilterFactory tupleFilterFactory,
            boolean retainInput, boolean retainNull, INullWriterFactory nullWriterFactory,
            ILocalResourceFactoryProvider localResourceFactoryProvider,
            ISearchOperationCallbackFactory searchOpCallbackFactory,
            IModificationOperationCallbackFactory modificationOpCallbackFactory) {
        super(spec, inputArity, outputArity, recDesc, storageManager, lifecycleManagerProvider, fileSplitProvider,
                dataflowHelperFactory, tupleFilterFactory, retainInput, retainNull, nullWriterFactory,
                localResourceFactoryProvider, searchOpCallbackFactory, modificationOpCallbackFactory);
        this.typeTraits = typeTraits;
        this.comparatorFactories = comparatorFactories;
        this.bloomFilterKeyFields = bloomFilterKeyFields;
    }

    public IBinaryComparatorFactory[] getTreeIndexComparatorFactories() {
        return comparatorFactories;
    }

    public ITypeTraits[] getTreeIndexTypeTraits() {
        return typeTraits;
    }

    public int[] getTreeIndexBloomFilterKeyFields() {
        return bloomFilterKeyFields;
    }

    @Override
    public boolean getRetainInput() {
        return retainInput;
    }

    @Override
    public ITupleFilterFactory getTupleFilterFactory() {
        return tupleFilterFactory;
    }
}