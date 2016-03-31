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
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;

public abstract class AbstractLSMIndexDataflowHelperFactory implements IIndexDataflowHelperFactory {
    protected static final long serialVersionUID = 1L;

    protected final IVirtualBufferCacheProvider virtualBufferCacheProvider;
    protected final ILSMMergePolicyFactory mergePolicyFactory;
    protected final Map<String, String> mergePolicyProperties;
    protected final ILSMOperationTrackerProvider opTrackerFactory;
    protected final ILSMIOOperationSchedulerProvider ioSchedulerProvider;
    protected final ILSMIOOperationCallbackFactory ioOpCallbackFactory;
    protected final double bloomFilterFalsePositiveRate;
    protected final ITypeTraits[] filterTypeTraits;
    protected final IBinaryComparatorFactory[] filterCmpFactories;
    protected final int[] filterFields;
    protected final boolean durable;

    public AbstractLSMIndexDataflowHelperFactory(IVirtualBufferCacheProvider virtualBufferCacheProvider,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationSchedulerProvider ioSchedulerProvider,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, double bloomFilterFalsePositiveRate,
            ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            boolean durable) {
        this.virtualBufferCacheProvider = virtualBufferCacheProvider;
        this.mergePolicyFactory = mergePolicyFactory;
        this.opTrackerFactory = opTrackerFactory;
        this.ioSchedulerProvider = ioSchedulerProvider;
        this.ioOpCallbackFactory = ioOpCallbackFactory;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.mergePolicyProperties = mergePolicyProperties;
        this.filterTypeTraits = filterTypeTraits;
        this.filterCmpFactories = filterCmpFactories;
        this.filterFields = filterFields;
        this.durable = durable;
    }
}
