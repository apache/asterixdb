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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.datagen.TupleGenerator;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.ConjunctiveSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.JaccardSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext.InvertedIndexType;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestUtils;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractInvertedIndexTest {
    protected final Logger LOGGER = LogManager.getLogger();

    protected final LSMInvertedIndexTestHarness harness = new LSMInvertedIndexTestHarness();

    protected final int NUM_DOCS_TO_INSERT = AccessMethodTestsConfig.LSM_INVINDEX_NUM_DOCS_TO_INSERT;
    protected final int[] SCAN_COUNT_ARRAY = new int[AccessMethodTestsConfig.LSM_INVINDEX_SCAN_COUNT_ARRAY_SIZE];

    protected final int TINY_WORKLOAD_NUM_DOC_QUERIES = AccessMethodTestsConfig.LSM_INVINDEX_TINY_NUM_DOC_QUERIES;
    protected final int TINY_WORKLOAD_NUM_RANDOM_QUERIES = AccessMethodTestsConfig.LSM_INVINDEX_TINY_NUM_RANDOM_QUERIES;

    // Note: The edit-distance search modifier is tested separately.
    protected final IInvertedIndexSearchModifier[] TEST_SEARCH_MODIFIERS = new IInvertedIndexSearchModifier[] {
            new ConjunctiveSearchModifier(), new JaccardSearchModifier(0.8f), new JaccardSearchModifier(0.5f) };

    protected final InvertedIndexType invIndexType;

    public AbstractInvertedIndexTest(InvertedIndexType invIndexType) {
        this.invIndexType = invIndexType;
    }

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    /**
     * Validates the index, and compares it against the expected index.
     * This test is only for verifying the integrity and correctness of the index,
     * it does not ensure the correctness of index searches.
     */
    protected void validateAndCheckIndex(LSMInvertedIndexTestContext testCtx) throws HyracksDataException {
        IIndex invIndex = testCtx.getIndex();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Validating index: " + invIndex);
        }
        // Validate index and compare against expected index.
        invIndex.validate();
        if (invIndexType == InvertedIndexType.INMEMORY || invIndexType == InvertedIndexType.ONDISK) {
            // This comparison method exercises different features of these types of inverted indexes.
            LSMInvertedIndexTestUtils.compareActualAndExpectedIndexes(testCtx);
        }
        LSMInvertedIndexTestUtils.compareActualAndExpectedIndexesRangeSearch(testCtx);
        if (invIndexType == InvertedIndexType.LSM || invIndexType == InvertedIndexType.PARTITIONED_LSM) {
            LSMInvertedIndex lsmIndex = (LSMInvertedIndex) invIndex;
            if (!lsmIndex.isMemoryComponentsAllocated() || lsmIndex.isCurrentMutableComponentEmpty()) {
                LSMInvertedIndexTestUtils.compareActualAndExpectedIndexesMergeSearch(testCtx);
            }
        }
    }

    /**
     * Runs a workload of queries using different search modifiers, and verifies the correctness of the results.
     */
    protected void runTinySearchWorkload(LSMInvertedIndexTestContext testCtx, TupleGenerator tupleGen)
            throws IOException {
        for (IInvertedIndexSearchModifier searchModifier : TEST_SEARCH_MODIFIERS) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Running test workload with: " + searchModifier.toString());
            }
            LSMInvertedIndexTestUtils.testIndexSearch(testCtx, tupleGen, harness.getRandom(),
                    TINY_WORKLOAD_NUM_DOC_QUERIES, TINY_WORKLOAD_NUM_RANDOM_QUERIES, searchModifier, SCAN_COUNT_ARRAY);
        }
    }
}
