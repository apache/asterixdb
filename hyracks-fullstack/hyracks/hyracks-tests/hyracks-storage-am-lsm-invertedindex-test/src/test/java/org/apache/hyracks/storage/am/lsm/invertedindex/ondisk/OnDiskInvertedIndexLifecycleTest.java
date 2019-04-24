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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.common.LSMInvertedIndexTestHarness;

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
        IBinaryComparatorFactory[] tokenCmpFactories =
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE };
        ITypeTraits[] invListTypeTraits = new ITypeTraits[] { IntegerPointable.TYPE_TRAITS };
        IBinaryComparatorFactory[] invListCmpFactories =
                new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE };
        IInvertedListBuilder invListBuilder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        FileReference btreeFile = harness.getIOManager()
                .resolveAbsolutePath(harness.getInvListsFileRef().getFile().getAbsolutePath() + "_btree");
        index = new OnDiskInvertedIndex(harness.getDiskBufferCache(), invListBuilder, invListTypeTraits,
                invListCmpFactories, tokenTypeTraits, tokenCmpFactories, harness.getInvListsFileRef(), btreeFile,
                harness.getMetadataPageManagerFactory());

    }

    @Override
    public void tearDown() throws Exception {
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
