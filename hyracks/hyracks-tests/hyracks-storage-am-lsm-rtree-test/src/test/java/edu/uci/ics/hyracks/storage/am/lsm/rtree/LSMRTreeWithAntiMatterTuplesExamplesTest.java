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

package edu.uci.ics.hyracks.storage.am.lsm.rtree;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.util.LSMRTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import edu.uci.ics.hyracks.storage.am.rtree.AbstractRTreeExamplesTest;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;

public class LSMRTreeWithAntiMatterTuplesExamplesTest extends AbstractRTreeExamplesTest {
    private final LSMRTreeTestHarness harness = new LSMRTreeTestHarness();

    @Override
    protected ITreeIndex createTreeIndex(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType) throws TreeIndexException {
        return LSMRTreeUtils.createLSMTreeWithAntiMatterTuples(harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), harness.getDiskFileMapProvider(), typeTraits,
                rtreeCmpFactories, btreeCmpFactories, valueProviderFactories, rtreePolicyType,
                harness.getMergePolicy(), harness.getOperationTracker(), harness.getIOScheduler(),
                harness.getIOOperationCallback(),
                LSMRTreeUtils.proposeBestLinearizer(typeTraits, rtreeCmpFactories.length));
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
