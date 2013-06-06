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

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.tests.am.common.ITreeIndexOperatorTestHelper;
import edu.uci.ics.hyracks.tests.am.rtree.RTreeSecondaryIndexSearchOperatorTest;

public class LSMRTreeSecondaryIndexSearchOperatorTest extends RTreeSecondaryIndexSearchOperatorTest {
    protected ITreeIndexOperatorTestHelper createTestHelper() throws HyracksException {
        return new LSMRTreeOperatorTestHelper(TestStorageManagerComponentHolder.getIOManager());
    }

    @Override
    protected IIndexDataflowHelperFactory createDataFlowHelperFactory(
            IPrimitiveValueProviderFactory[] secondaryValueProviderFactories, RTreePolicyType rtreePolicyType,
            IBinaryComparatorFactory[] btreeComparatorFactories, ILinearizeComparatorFactory linearizerCmpFactory) {
        return ((LSMRTreeOperatorTestHelper) testHelper).createDataFlowHelperFactory(secondaryValueProviderFactories,
                rtreePolicyType, btreeComparatorFactories, linearizerCmpFactory);
    }
}