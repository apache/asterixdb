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

package org.apache.hyracks.tests.am.lsm.rtree;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.tests.am.common.ITreeIndexOperatorTestHelper;
import org.apache.hyracks.tests.am.rtree.RTreeSecondaryIndexSearchOperatorTest;

public class LSMRTreeWithAntiMatterTuplesSecondaryIndexSearchOperatorTest extends RTreeSecondaryIndexSearchOperatorTest {
    public LSMRTreeWithAntiMatterTuplesSecondaryIndexSearchOperatorTest() {
            this.rTreeType = RTreeType.LSMRTREE_WITH_ANTIMATTER;
        }
    
    protected ITreeIndexOperatorTestHelper createTestHelper() throws HyracksException {
        return new LSMRTreeWithAntiMatterTuplesOperatorTestHelper(TestStorageManagerComponentHolder.getIOManager());
    }

    @Override
    protected IIndexDataflowHelperFactory createDataFlowHelperFactory(
            IPrimitiveValueProviderFactory[] secondaryValueProviderFactories, RTreePolicyType rtreePolicyType,
            IBinaryComparatorFactory[] btreeComparatorFactories, ILinearizeComparatorFactory linearizerCmpFactory,
            int[] btreeFields) {
        return ((LSMRTreeWithAntiMatterTuplesOperatorTestHelper) testHelper).createDataFlowHelperFactory(
                secondaryValueProviderFactories, rtreePolicyType, btreeComparatorFactories, linearizerCmpFactory);
    }
}