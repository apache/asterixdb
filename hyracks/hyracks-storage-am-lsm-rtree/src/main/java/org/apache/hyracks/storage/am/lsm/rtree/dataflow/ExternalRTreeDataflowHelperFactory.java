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
package org.apache.hyracks.storage.am.lsm.rtree.dataflow;

import java.util.Map;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;

public class ExternalRTreeDataflowHelperFactory extends LSMRTreeDataflowHelperFactory {

    private static final long serialVersionUID = 1L;
    private final int version;

    public ExternalRTreeDataflowHelperFactory(IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, IBinaryComparatorFactory[] btreeComparatorFactories,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationSchedulerProvider ioSchedulerProvider,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILinearizeComparatorFactory linearizeCmpFactory,
            double bloomFilterFalsePositiveRate, int[] btreeFields, int version, boolean durable) {
        super(valueProviderFactories, rtreePolicyType, btreeComparatorFactories, null, mergePolicyFactory,
                mergePolicyProperties, opTrackerFactory, ioSchedulerProvider, ioOpCallbackFactory, linearizeCmpFactory,
                bloomFilterFalsePositiveRate, null, btreeFields, null, null, null, durable);
        this.version = version;
    }

    @Override
    public IndexDataflowHelper createIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition) {
        return new ExternalRTreeDataflowHelper(opDesc, ctx, partition, bloomFilterFalsePositiveRate,
                btreeComparatorFactories, valueProviderFactories, rtreePolicyType,
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, ctx), opTrackerFactory,
                ioSchedulerProvider.getIOScheduler(ctx), ioOpCallbackFactory, linearizeCmpFactory, btreeFields,
                version, durable);
    }
}
