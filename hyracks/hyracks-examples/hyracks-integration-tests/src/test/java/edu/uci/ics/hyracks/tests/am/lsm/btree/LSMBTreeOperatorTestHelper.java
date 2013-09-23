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

package edu.uci.ics.hyracks.tests.am.lsm.btree;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ThreadCountingOperationTrackerProvider;
import edu.uci.ics.hyracks.tests.am.common.LSMTreeOperatorTestHelper;

public class LSMBTreeOperatorTestHelper extends LSMTreeOperatorTestHelper {

    private static final Map<String, String> MERGE_POLICY_PROPERTIES;
    static {
        MERGE_POLICY_PROPERTIES = new HashMap<String, String>();
        MERGE_POLICY_PROPERTIES.put("num-components", "3");
    }
    
    public LSMBTreeOperatorTestHelper(IOManager ioManager) {
        super(ioManager);
    }

    public IIndexDataflowHelperFactory createDataFlowHelperFactory() {
        return new LSMBTreeDataflowHelperFactory(virtualBufferCacheProvider, new ConstantMergePolicyFactory(),
                MERGE_POLICY_PROPERTIES, ThreadCountingOperationTrackerProvider.INSTANCE,
                SynchronousSchedulerProvider.INSTANCE, NoOpIOOperationCallback.INSTANCE,
                DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE);
    }

}
