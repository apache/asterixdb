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

package edu.uci.ics.hyracks.storage.am.lsm.common.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;

public abstract class AbstractLSMIndexDataflowHelper extends IndexDataflowHelper {

    protected static double DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;

    protected final double bloomFilterFalsePositiveRate;

    protected final IVirtualBufferCache virtualBufferCache;
    protected final ILSMMergePolicy mergePolicy;
    protected final ILSMIOOperationScheduler ioScheduler;
    protected final ILSMOperationTrackerProvider opTrackerFactory;
    protected final ILSMIOOperationCallbackProvider ioOpCallbackProvider;

    public AbstractLSMIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            IVirtualBufferCache virtualBufferCache, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackProvider ioOpCallbackProvider) {
        this(opDesc, ctx, partition, virtualBufferCache, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE, mergePolicy,
                opTrackerFactory, ioScheduler, ioOpCallbackProvider);
    }

    public AbstractLSMIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            IVirtualBufferCache virtualBufferCache, double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackProvider ioOpCallbackProvider) {
        super(opDesc, ctx, partition);
        this.virtualBufferCache = virtualBufferCache;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.mergePolicy = mergePolicy;
        this.opTrackerFactory = opTrackerFactory;
        this.ioScheduler = ioScheduler;
        this.ioOpCallbackProvider = ioOpCallbackProvider;
    }
}
