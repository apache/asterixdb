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

package org.apache.hyracks.storage.am.lsm.invertedindex;

import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.datagen.TupleGenerator;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.common.AbstractInvertedIndexLoadTest;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext.InvertedIndexType;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestUtils;
import org.apache.hyracks.storage.common.IIndex;

public class PartitionedLSMInvertedIndexMergeTest extends AbstractInvertedIndexLoadTest {

    private final int maxTreesToMerge = AccessMethodTestsConfig.LSM_INVINDEX_MAX_TREES_TO_MERGE;

    public PartitionedLSMInvertedIndexMergeTest() {
        super(InvertedIndexType.PARTITIONED_LSM, false);
    }

    @Override
    protected void runTest(LSMInvertedIndexTestContext testCtx, TupleGenerator tupleGen)
            throws IOException, HyracksDataException {
        IIndex invIndex = testCtx.getIndex();
        invIndex.create();
        invIndex.activate();
        ILSMIndexAccessor invIndexAccessor =
                (ILSMIndexAccessor) invIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);

        for (int i = 0; i < maxTreesToMerge; i++) {
            for (int j = 0; j < i; j++) {
                LSMInvertedIndexTestUtils.insertIntoInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT);
                // Deactivate and the re-activate the index to force it flush its in memory component
                invIndex.deactivate();
                invIndex.activate();
            }
            // Perform merge.
            invIndexAccessor.scheduleMerge(((LSMInvertedIndex) invIndex).getDiskComponents());
            validateAndCheckIndex(testCtx);
            runTinySearchWorkload(testCtx, tupleGen);
        }

        invIndex.deactivate();
        invIndex.destroy();
    }
}
