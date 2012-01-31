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

package edu.uci.ics.hyracks.storage.am.lsm.btree;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.tests.OrderedIndexExamplesTest;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;

public class LSMBTreeExamplesTest extends OrderedIndexExamplesTest {
	private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();
	
	@Override
	protected ITreeIndex createTreeIndex(ITypeTraits[] typeTraits,
			IBinaryComparatorFactory[] cmpFactory) throws TreeIndexException {
		return LSMBTreeUtils.createLSMTree(harness.getMemBufferCache(),
				harness.getMemFreePageManager(), harness.getOnDiskDir(),
				harness.getDiskBufferCache(), harness.getDiskFileMapProvider(),
				typeTraits, cmpFactory);
	}

	@Override
	protected int getIndexFileId() {
		return harness.getFileId();
	}

	@Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }
}
