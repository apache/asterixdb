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
package org.apache.hyracks.storage.am.lsm.vector.dataflow;

import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LsmResourceFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleBuilderFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunctionFactory;
import org.apache.hyracks.storage.am.vector.utils.CrossPollinationConfig;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.IStorageManager;

public class LSMVTreeLocalResourceFactory extends LsmResourceFactory {

    private static final long serialVersionUID = 2L;

    protected final int vectorDimensions;
    protected final int[] vectorFields;
    protected final boolean atomic;
    protected final IVTreeBinaryAccessorFactory vectorAccessorFactory;
    protected final int numPrimaryKeyFields;
    protected final int numIncludeFields;
    protected final IVTreeDataTupleBuilderFactory dataTupleBuilderFactory;

    /** Distance-function factory supplied at DDL time; threaded onto {@link LSMVTreeLocalResource} for persistence. */
    protected final IVTreeDistanceFunctionFactory distanceFunctionFactory;

    /** Cross-pollination placement config supplied at DDL time; threaded onto {@link LSMVTreeLocalResource}. */
    protected final CrossPollinationConfig crossPollination;

    public LSMVTreeLocalResourceFactory(IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerFactory, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable, int vectorDimensions, int[] vectorFields,
            ITypeTraits nullTypeTraits, INullIntrospector nullIntrospector, boolean atomic,
            IVTreeBinaryAccessorFactory vectorAccessorFactory, int numPrimaryKeyFields, int numIncludeFields,
            IVTreeDataTupleBuilderFactory dataTupleBuilderFactory,
            IVTreeDistanceFunctionFactory distanceFunctionFactory, CrossPollinationConfig crossPollination) {
        super(storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerFactory, ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, durable, nullTypeTraits,
                nullIntrospector);
        this.vectorDimensions = vectorDimensions;
        this.vectorFields = vectorFields;
        this.atomic = atomic;
        this.distanceFunctionFactory = distanceFunctionFactory;
        this.crossPollination = crossPollination != null ? crossPollination : CrossPollinationConfig.LEGACY;
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.numPrimaryKeyFields = numPrimaryKeyFields;
        this.numIncludeFields = numIncludeFields;
        this.dataTupleBuilderFactory = dataTupleBuilderFactory;
    }

    @Override
    public IResource createResource(FileReference fileRef) {
        return new LSMVTreeLocalResource(fileRef.getRelativePath(), storageManager, typeTraits, cmpFactories,
                filterTypeTraits, filterCmpFactories, filterFields, opTrackerProvider, ioOpCallbackFactory,
                pageWriteCallbackFactory, metadataPageManagerFactory, vbcProvider, ioSchedulerProvider,
                mergePolicyFactory, mergePolicyProperties, durable, vectorDimensions, vectorFields, nullTypeTraits,
                nullIntrospector, atomic, vectorAccessorFactory, numPrimaryKeyFields, numIncludeFields,
                dataTupleBuilderFactory, distanceFunctionFactory, null, null, null, null, null, null, crossPollination);
    }
}
