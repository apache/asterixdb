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

package org.apache.hyracks.tests.am.lsm.rtree;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.impls.ThreadCountingOperationTrackerProvider;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.LSMRTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.tests.am.common.LSMTreeOperatorTestHelper;

public class LSMRTreeOperatorTestHelper extends LSMTreeOperatorTestHelper {

    private static final Map<String, String> MERGE_POLICY_PROPERTIES;
    static {
        MERGE_POLICY_PROPERTIES = new HashMap<String, String>();
        MERGE_POLICY_PROPERTIES.put("num-components", "3");
    }

    public LSMRTreeOperatorTestHelper(IOManager ioManager) {
        super(ioManager);
    }

    public IIndexDataflowHelperFactory createDataFlowHelperFactory(
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            IBinaryComparatorFactory[] btreeComparatorFactories, ILinearizeComparatorFactory linearizerCmpFactory,
            int[] btreeFields) {
        return new LSMRTreeDataflowHelperFactory(valueProviderFactories, rtreePolicyType, btreeComparatorFactories,
                virtualBufferCacheProvider, new ConstantMergePolicyFactory(), MERGE_POLICY_PROPERTIES,
                ThreadCountingOperationTrackerProvider.INSTANCE, SynchronousSchedulerProvider.INSTANCE,
                NoOpIOOperationCallback.INSTANCE, linearizerCmpFactory, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE, null,
                btreeFields, null, null, null, true);
    }
}
