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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk;

import java.io.File;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.common.LSMInvertedIndexTestHarness;

public class OnDiskInvertedIndexLifecycleTest extends AbstractIndexLifecycleTest {

    private final LSMInvertedIndexTestHarness harness = new LSMInvertedIndexTestHarness();
    private ITreeIndexFrame frame = null;

    @Override
    protected boolean persistentStateExists() throws Exception {
        return harness.getInvListsFileRef().getFile().exists()
                && ((OnDiskInvertedIndex) index).getBTree().getFileReference().getFile().exists();
    }

    @Override
    protected boolean isEmptyIndex() throws Exception {
        if (frame == null) {
            frame = ((OnDiskInvertedIndex) index).getBTree().getLeafFrameFactory().createFrame();
        }
        return ((OnDiskInvertedIndex) index).getBTree().isEmptyTree(frame);
    }

    @Override
    public void setup() throws Exception {
        harness.setUp();
        ITypeTraits[] tokenTypeTraits = new ITypeTraits[] { UTF8StringPointable.TYPE_TRAITS };
        IBinaryComparatorFactory[] tokenCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                .of(UTF8StringPointable.FACTORY) };
        ITypeTraits[] invListTypeTraits = new ITypeTraits[] { IntegerPointable.TYPE_TRAITS };
        IBinaryComparatorFactory[] invListCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                .of(IntegerPointable.FACTORY) };
        IInvertedListBuilder invListBuilder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        FileReference btreeFile = new FileReference(new File(harness.getInvListsFileRef().getFile().getPath()
                + "_btree"));
        index = new OnDiskInvertedIndex(harness.getDiskBufferCache(), harness.getDiskFileMapProvider(), invListBuilder,
                invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories,
                harness.getInvListsFileRef(), btreeFile);

    }

    @Override
    public void tearDown() throws Exception {
        try {
            index.deactivate();
        } catch (Exception e) {
        } finally {
            index.destroy();
        }
        harness.tearDown();
    }

    @Override
    protected void performInsertions() throws Exception {
        // Do nothing.
    }

    @Override
    protected void checkInsertions() throws Exception {
        // Do nothing.
    }

    @Override
    protected void clearCheckableInsertions() throws Exception {
        // Do nothing.
    }
}
