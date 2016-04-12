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

package org.apache.hyracks.storage.am.lsm.rtree;

import org.junit.After;
import org.junit.Before;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.lsm.rtree.util.LSMRTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;

public class LSMRTreeWithAntiMatterTuplesExamplesTest extends AbstractLSMRTreeExamplesTest {
    private final LSMRTreeTestHarness harness = new LSMRTreeTestHarness();

    public LSMRTreeWithAntiMatterTuplesExamplesTest() {
        super();
        this.rTreeType = RTreeType.LSMRTREE_WITH_ANTIMATTER;
    }

    @Override
    protected ITreeIndex createTreeIndex(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, int[] rtreeFields, int[] btreeFields, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields) throws TreeIndexException {
        return LSMRTreeUtils.createLSMTreeWithAntiMatterTuples(harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), harness.getDiskFileMapProvider(), typeTraits,
                rtreeCmpFactories, btreeCmpFactories, valueProviderFactories, rtreePolicyType,
                harness.getMergePolicy(), harness.getOperationTracker(), harness.getIOScheduler(),
                harness.getIOOperationCallback(),
                LSMRTreeUtils.proposeBestLinearizer(typeTraits, rtreeCmpFactories.length), rtreeFields,
                filterTypeTraits, filterCmpFactories, filterFields, true);
    }

    @Before
    public void setUp() throws HyracksException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

}
