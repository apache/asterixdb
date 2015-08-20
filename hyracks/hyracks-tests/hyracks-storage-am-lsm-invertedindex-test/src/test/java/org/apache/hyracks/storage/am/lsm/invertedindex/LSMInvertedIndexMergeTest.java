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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex;

import java.io.IOException;

import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleGenerator;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.common.AbstractInvertedIndexLoadTest;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext.InvertedIndexType;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestUtils;

public class LSMInvertedIndexMergeTest extends AbstractInvertedIndexLoadTest {

    private final int maxTreesToMerge = AccessMethodTestsConfig.LSM_INVINDEX_MAX_TREES_TO_MERGE;

    public LSMInvertedIndexMergeTest() {
        super(InvertedIndexType.LSM, false);
    }

    @Override
    protected void runTest(LSMInvertedIndexTestContext testCtx, TupleGenerator tupleGen) throws IOException,
            IndexException {
        IIndex invIndex = testCtx.getIndex();
        invIndex.create();
        invIndex.activate();
        ILSMIndexAccessor invIndexAccessor = (ILSMIndexAccessor) invIndex.createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);

        for (int i = 0; i < maxTreesToMerge; i++) {
            for (int j = 0; j < i; j++) {
                LSMInvertedIndexTestUtils.insertIntoInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT);
                // Deactivate and the re-activate the index to force it flush its in memory component
                invIndex.deactivate();
                invIndex.activate();
            }
            // Perform merge.
            invIndexAccessor.scheduleMerge(NoOpIOOperationCallback.INSTANCE,
                    ((LSMInvertedIndex) invIndex).getImmutableComponents());
            validateAndCheckIndex(testCtx);
            runTinySearchWorkload(testCtx, tupleGen);
        }

        invIndex.deactivate();
        invIndex.destroy();
    }
}
