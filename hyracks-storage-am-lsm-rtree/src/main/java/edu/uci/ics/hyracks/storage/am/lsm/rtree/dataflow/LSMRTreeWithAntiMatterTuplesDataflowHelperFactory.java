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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushControllerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;

public class LSMRTreeWithAntiMatterTuplesDataflowHelperFactory implements IIndexDataflowHelperFactory {

    private static final long serialVersionUID = 1L;

    private final IBinaryComparatorFactory[] btreeComparatorFactories;
    private final IPrimitiveValueProviderFactory[] valueProviderFactories;
    private final RTreePolicyType rtreePolicyType;
    private final ILSMFlushControllerProvider flushControllerProvider;
    private final ILSMMergePolicyProvider mergePolicyProvider;
    private final ILSMOperationTrackerProvider opTrackerProvider;
    private final ILSMIOOperationSchedulerProvider ioSchedulerProvider;
    private final ILinearizeComparatorFactory linearizeCmpFactory;

    public LSMRTreeWithAntiMatterTuplesDataflowHelperFactory(IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, IBinaryComparatorFactory[] btreeComparatorFactories,
            ILSMFlushControllerProvider flushControllerProvider, ILSMMergePolicyProvider mergePolicyProvider,
            ILSMOperationTrackerProvider opTrackerProvider, ILSMIOOperationSchedulerProvider ioSchedulerProvider,
            ILinearizeComparatorFactory linearizeCmpFactory) {
        this.btreeComparatorFactories = btreeComparatorFactories;
        this.valueProviderFactories = valueProviderFactories;
        this.rtreePolicyType = rtreePolicyType;
        this.flushControllerProvider = flushControllerProvider;
        this.mergePolicyProvider = mergePolicyProvider;
        this.ioSchedulerProvider = ioSchedulerProvider;
        this.opTrackerProvider = opTrackerProvider;
        this.linearizeCmpFactory = linearizeCmpFactory;
    }

    @Override
    public IndexDataflowHelper createIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition) {
        return new LSMRTreeWithAntiMatterTuplesDataflowHelper(opDesc, ctx, partition, btreeComparatorFactories,
                valueProviderFactories, rtreePolicyType, flushControllerProvider.getFlushController(ctx),
                mergePolicyProvider.getMergePolicy(ctx), opTrackerProvider.getOperationTracker(ctx),
                ioSchedulerProvider.getIOScheduler(ctx), linearizeCmpFactory);
    }
}
