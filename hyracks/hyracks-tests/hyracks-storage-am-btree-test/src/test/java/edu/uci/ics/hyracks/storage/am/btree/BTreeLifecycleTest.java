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

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestContext;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.IIndexTestContext;
import edu.uci.ics.hyracks.storage.am.common.TreeIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;

public class BTreeLifecycleTest extends AbstractIndexLifecycleTest {
    private final BTreeTestHarness harness = new BTreeTestHarness();
    private final TreeIndexTestUtils titu = new OrderedIndexTestUtils();

    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE };

    private ITreeIndexFrame frame = null;

    @SuppressWarnings("rawtypes")
    private IIndexTestContext<? extends CheckTuple> testCtx;

    @Override
    public void setup() throws Exception {
        harness.setUp();
        testCtx = BTreeTestContext.create(harness.getBufferCache(), harness.getFileMapProvider(),
                harness.getFileReference(), fieldSerdes, fieldSerdes.length, BTreeLeafFrameType.REGULAR_NSM);
        index = testCtx.getIndex();
    }

    @Override
    public void tearDown() throws HyracksDataException {
        try {
            testCtx.getIndex().deactivate();
        } catch (Exception e) {
        } finally {
            testCtx.getIndex().destroy();
        }
        harness.tearDown();
    }

    @Override
    protected boolean persistentStateExists() {
        return harness.getFileReference().getFile().exists();
    }

    @Override
    protected boolean isEmptyIndex() throws HyracksDataException {
        BTree btree = (BTree) testCtx.getIndex();
        if (frame == null) {
            frame = btree.getInteriorFrameFactory().createFrame();
        }
        return btree.isEmptyTree(frame);
    }

    @Override
    protected void performInsertions() throws Exception {
        titu.insertIntTuples(testCtx, 10, harness.getRandom());
    }

    @Override
    protected void checkInsertions() throws Exception {
        titu.checkScan(testCtx);
    }

    @Override
    protected void clearCheckableInsertions() throws Exception {
        testCtx.getCheckTuples().clear();
    }
}
