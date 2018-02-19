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
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.storage.am.common.datagen.TupleGenerator;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.ConjunctiveSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.EditDistanceSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.JaccardSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext.InvertedIndexType;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestUtils;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public abstract class AbstractInvertedIndexSearchTest extends AbstractInvertedIndexTest {

    protected final Logger LOGGER = LogManager.getLogger();

    protected int NUM_DOC_QUERIES = AccessMethodTestsConfig.LSM_INVINDEX_NUM_DOC_QUERIES;
    protected int NUM_RANDOM_QUERIES = AccessMethodTestsConfig.LSM_INVINDEX_NUM_RANDOM_QUERIES;
    protected final boolean bulkLoad;

    public AbstractInvertedIndexSearchTest(InvertedIndexType invIndexType, boolean bulkLoad) {
        super(invIndexType);
        this.bulkLoad = bulkLoad;
    }

    protected void runTest(LSMInvertedIndexTestContext testCtx, TupleGenerator tupleGen,
            List<IInvertedIndexSearchModifier> searchModifiers) throws IOException {
        IIndex invIndex = testCtx.getIndex();
        if ((invIndexType != InvertedIndexType.LSM) && (invIndexType != InvertedIndexType.PARTITIONED_LSM)
                || !bulkLoad) {
            invIndex.create();
            invIndex.activate();
        }
        if (bulkLoad) {
            if ((invIndexType != InvertedIndexType.LSM) && (invIndexType != InvertedIndexType.PARTITIONED_LSM)) {
                LSMInvertedIndexTestUtils.bulkLoadInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT, false);
            } else {
                LSMInvertedIndexTestUtils.bulkLoadInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT, true);
            }
        } else {
            LSMInvertedIndexTestUtils.insertIntoInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT);
        }
        invIndex.validate();

        for (IInvertedIndexSearchModifier searchModifier : searchModifiers) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Running searches with: " + searchModifier.toString());
            }
            LSMInvertedIndexTestUtils.testIndexSearch(testCtx, tupleGen, harness.getRandom(), NUM_DOC_QUERIES,
                    NUM_RANDOM_QUERIES, searchModifier, SCAN_COUNT_ARRAY);
        }

        invIndex.deactivate();
        invIndex.destroy();
    }

    private void testWordInvIndexIndex(LSMInvertedIndexTestContext testCtx) throws IOException {
        TupleGenerator tupleGen = LSMInvertedIndexTestUtils.createStringDocumentTupleGen(harness.getRandom());
        List<IInvertedIndexSearchModifier> searchModifiers = new ArrayList<>();
        searchModifiers.add(new ConjunctiveSearchModifier());
        searchModifiers.add(new JaccardSearchModifier(1.0f));
        searchModifiers.add(new JaccardSearchModifier(0.8f));
        searchModifiers.add(new JaccardSearchModifier(0.5f));
        runTest(testCtx, tupleGen, searchModifiers);
    }

    private void testNGramInvIndexIndex(LSMInvertedIndexTestContext testCtx) throws IOException {
        TupleGenerator tupleGen = LSMInvertedIndexTestUtils.createPersonNamesTupleGen(harness.getRandom());
        List<IInvertedIndexSearchModifier> searchModifiers = new ArrayList<>();
        searchModifiers.add(new ConjunctiveSearchModifier());
        searchModifiers.add(new JaccardSearchModifier(1.0f));
        searchModifiers.add(new JaccardSearchModifier(0.8f));
        searchModifiers.add(new JaccardSearchModifier(0.5f));
        searchModifiers.add(new EditDistanceSearchModifier(LSMInvertedIndexTestUtils.TEST_GRAM_LENGTH, 0));
        searchModifiers.add(new EditDistanceSearchModifier(LSMInvertedIndexTestUtils.TEST_GRAM_LENGTH, 1));
        searchModifiers.add(new EditDistanceSearchModifier(LSMInvertedIndexTestUtils.TEST_GRAM_LENGTH, 2));
        searchModifiers.add(new EditDistanceSearchModifier(LSMInvertedIndexTestUtils.TEST_GRAM_LENGTH, 3));
        runTest(testCtx, tupleGen, searchModifiers);
    }

    @Test
    public void wordTokensInvIndexTest() throws IOException {
        LSMInvertedIndexTestContext testCtx =
                LSMInvertedIndexTestUtils.createWordInvIndexTestContext(harness, invIndexType);
        testWordInvIndexIndex(testCtx);
    }

    @Test
    public void hashedWordTokensInvIndexTest() throws IOException {
        LSMInvertedIndexTestContext testCtx =
                LSMInvertedIndexTestUtils.createHashedWordInvIndexTestContext(harness, invIndexType);
        testWordInvIndexIndex(testCtx);
    }

    @Test
    public void ngramTokensInvIndexTest() throws IOException {
        LSMInvertedIndexTestContext testCtx =
                LSMInvertedIndexTestUtils.createNGramInvIndexTestContext(harness, invIndexType);
        testNGramInvIndexIndex(testCtx);
    }

    @Test
    public void hashedNGramTokensInvIndexTest() throws IOException {
        LSMInvertedIndexTestContext testCtx =
                LSMInvertedIndexTestUtils.createHashedNGramInvIndexTestContext(harness, invIndexType);
        testNGramInvIndexIndex(testCtx);
    }

}
