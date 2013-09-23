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

package edu.uci.ics.hyracks.tests.am.lsm.rtree;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ThreadCountingOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.dataflow.LSMRTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.tests.am.common.LSMTreeOperatorTestHelper;

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
            IBinaryComparatorFactory[] btreeComparatorFactories, ILinearizeComparatorFactory linearizerCmpFactory) {
        return new LSMRTreeDataflowHelperFactory(valueProviderFactories, rtreePolicyType, btreeComparatorFactories,
                virtualBufferCacheProvider, new ConstantMergePolicyFactory(), MERGE_POLICY_PROPERTIES,
                ThreadCountingOperationTrackerProvider.INSTANCE, SynchronousSchedulerProvider.INSTANCE,
                NoOpIOOperationCallback.INSTANCE, linearizerCmpFactory, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE);
    }
}
