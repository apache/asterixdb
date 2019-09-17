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
package org.apache.hyracks.storage.am.lsm.common.dataflow;

import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;

public abstract class LsmResourceFactory implements IResourceFactory {

    private static final long serialVersionUID = 1L;
    protected final IStorageManager storageManager;
    protected final ITypeTraits[] typeTraits;
    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final ITypeTraits[] filterTypeTraits;
    protected final IBinaryComparatorFactory[] filterCmpFactories;
    protected final int[] filterFields;
    protected final ILSMOperationTrackerFactory opTrackerProvider;
    protected final ILSMIOOperationCallbackFactory ioOpCallbackFactory;
    protected final ILSMPageWriteCallbackFactory pageWriteCallbackFactory;
    protected final IMetadataPageManagerFactory metadataPageManagerFactory;
    protected final IVirtualBufferCacheProvider vbcProvider;
    protected final ILSMIOOperationSchedulerProvider ioSchedulerProvider;
    protected final ILSMMergePolicyFactory mergePolicyFactory;
    protected final Map<String, String> mergePolicyProperties;
    protected final boolean durable;

    public LsmResourceFactory(IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable) {
        this.storageManager = storageManager;
        this.typeTraits = typeTraits;
        this.cmpFactories = cmpFactories;
        this.filterTypeTraits = filterTypeTraits;
        this.filterCmpFactories = filterCmpFactories;
        this.filterFields = filterFields;
        this.opTrackerProvider = opTrackerProvider;
        this.ioOpCallbackFactory = ioOpCallbackFactory;
        this.pageWriteCallbackFactory = pageWriteCallbackFactory;
        this.metadataPageManagerFactory = metadataPageManagerFactory;
        this.vbcProvider = vbcProvider;
        this.ioSchedulerProvider = ioSchedulerProvider;
        this.mergePolicyFactory = mergePolicyFactory;
        this.mergePolicyProperties = mergePolicyProperties;
        this.durable = durable;
    }
}
