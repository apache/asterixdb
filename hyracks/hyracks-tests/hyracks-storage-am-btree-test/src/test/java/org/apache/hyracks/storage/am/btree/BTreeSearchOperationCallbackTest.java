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
package edu.uci.ics.hyracks.storage.am.btree;

import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;

public class BTreeSearchOperationCallbackTest extends AbstractSearchOperationCallbackTest {
    private final BTreeTestHarness harness;

    public BTreeSearchOperationCallbackTest() {
        harness = new BTreeTestHarness();
    }

    @Override
    protected void createIndexInstance() throws Exception {
        index = BTreeUtils.createBTree(harness.getBufferCache(), harness.getFileMapProvider(),
                SerdeUtils.serdesToTypeTraits(keySerdes),
                SerdeUtils.serdesToComparatorFactories(keySerdes, keySerdes.length), BTreeLeafFrameType.REGULAR_NSM,
                harness.getFileReference());
    }

    @Override
    public void setup() throws Exception {
        harness.setUp();
        super.setup();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        harness.tearDown();
    }

}
