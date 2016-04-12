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

import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;

public abstract class AbstractLSMIndexDataflowHelper extends IndexDataflowHelper {

    protected static double DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;

    protected final double bloomFilterFalsePositiveRate;

    protected final List<IVirtualBufferCache> virtualBufferCaches;
    protected final ILSMMergePolicy mergePolicy;
    protected final ILSMIOOperationScheduler ioScheduler;
    protected final ILSMOperationTrackerProvider opTrackerFactory;
    protected final ILSMIOOperationCallbackFactory ioOpCallbackFactory;
    protected final ITypeTraits[] filterTypeTraits;
    protected final IBinaryComparatorFactory[] filterCmpFactories;
    protected final int[] filterFields;

    public AbstractLSMIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            List<IVirtualBufferCache> virtualBufferCaches, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields, boolean durable) {
        this(opDesc, ctx, partition, virtualBufferCaches, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE, mergePolicy,
                opTrackerFactory, ioScheduler, ioOpCallbackFactory, filterTypeTraits, filterCmpFactories, filterFields,
                durable);
    }

    public AbstractLSMIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            List<IVirtualBufferCache> virtualBufferCaches, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerProvider opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            boolean durable) {
        super(opDesc, ctx, partition, durable);
        this.virtualBufferCaches = virtualBufferCaches;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.mergePolicy = mergePolicy;
        this.opTrackerFactory = opTrackerFactory;
        this.ioScheduler = ioScheduler;
        this.ioOpCallbackFactory = ioOpCallbackFactory;
        this.filterTypeTraits = filterTypeTraits;
        this.filterCmpFactories = filterCmpFactories;
        this.filterFields = filterFields;
    }
}
