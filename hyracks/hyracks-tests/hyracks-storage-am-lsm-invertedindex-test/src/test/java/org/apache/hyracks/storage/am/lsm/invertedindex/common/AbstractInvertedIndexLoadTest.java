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

package org.apache.hyracks.storage.am.lsm.invertedindex.common;

import java.io.IOException;

import org.junit.Test;

import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.datagen.TupleGenerator;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext.InvertedIndexType;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestUtils;

public abstract class AbstractInvertedIndexLoadTest extends AbstractInvertedIndexTest {

    protected final boolean bulkLoad;

    public AbstractInvertedIndexLoadTest(InvertedIndexType invIndexType, boolean bulkLoad) {
        super(invIndexType);
        this.bulkLoad = bulkLoad;
    }

    protected void runTest(LSMInvertedIndexTestContext testCtx, TupleGenerator tupleGen) throws IOException,
            IndexException {
        IIndex invIndex = testCtx.getIndex();
        invIndex.create();
        invIndex.activate();

        if (bulkLoad) {
            LSMInvertedIndexTestUtils.bulkLoadInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT);
        } else {
            LSMInvertedIndexTestUtils.insertIntoInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT);
        }
        validateAndCheckIndex(testCtx);
        runTinySearchWorkload(testCtx, tupleGen);

        invIndex.deactivate();
        invIndex.destroy();
    }

    @Test
    public void wordTokensInvIndexTest() throws IOException, IndexException {
        LSMInvertedIndexTestContext testCtx = LSMInvertedIndexTestUtils.createWordInvIndexTestContext(harness,
                invIndexType);
        TupleGenerator tupleGen = LSMInvertedIndexTestUtils.createStringDocumentTupleGen(harness.getRandom());
        runTest(testCtx, tupleGen);
    }

    @Test
    public void hashedWordTokensInvIndexTest() throws IOException, IndexException {
        LSMInvertedIndexTestContext testCtx = LSMInvertedIndexTestUtils.createHashedWordInvIndexTestContext(harness,
                invIndexType);
        TupleGenerator tupleGen = LSMInvertedIndexTestUtils.createStringDocumentTupleGen(harness.getRandom());
        runTest(testCtx, tupleGen);
    }

    @Test
    public void ngramTokensInvIndexTest() throws IOException, IndexException {
        LSMInvertedIndexTestContext testCtx = LSMInvertedIndexTestUtils.createNGramInvIndexTestContext(harness,
                invIndexType);
        TupleGenerator tupleGen = LSMInvertedIndexTestUtils.createPersonNamesTupleGen(harness.getRandom());
        runTest(testCtx, tupleGen);
    }

    @Test
    public void hashedNGramTokensInvIndexTest() throws IOException, IndexException {
        LSMInvertedIndexTestContext testCtx = LSMInvertedIndexTestUtils.createHashedNGramInvIndexTestContext(harness,
                invIndexType);
        TupleGenerator tupleGen = LSMInvertedIndexTestUtils.createPersonNamesTupleGen(harness.getRandom());
        runTest(testCtx, tupleGen);
    }
}
